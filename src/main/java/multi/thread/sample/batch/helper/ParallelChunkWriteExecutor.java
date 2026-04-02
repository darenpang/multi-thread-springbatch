package multi.thread.sample.batch.helper;

import lombok.NonNull;
import multi.thread.sample.batch.config.ParallelWriterProperties;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

@Component
public class ParallelChunkWriteExecutor {
    private final ThreadPoolTaskExecutor executor;
    private final PlatformTransactionManager platformTransactionManager;
    private final Semaphore globalSemaphore;
    private final int threadPoolSize;

    // Constructorで注入
    // ThreadPoolTaskExecutorは名前指定
    public ParallelChunkWriteExecutor(
            @Qualifier("parallelWriter") ThreadPoolTaskExecutor executor,
            PlatformTransactionManager platformTransactionManager,
            ParallelWriterProperties   parallelWriterProperties) {
        this.executor = executor;
        this.platformTransactionManager = platformTransactionManager;
        this.globalSemaphore = new Semaphore(parallelWriterProperties.getThreadPoolSize());
        this.threadPoolSize = parallelWriterProperties.getThreadPoolSize();
    }

    public <T> int execute(
            @NonNull Collection<? extends T> items,
            int maxConcurrency,
            @NonNull Consumer<List<T>> partitionWriter
    ) {
        if (maxConcurrency <= 0) {
            throw new IllegalArgumentException("maxConcurrency must be greater than 0");
        } else if (maxConcurrency > threadPoolSize) {
            maxConcurrency = threadPoolSize;
        }
        if (items.isEmpty()) {
            return 0;
        }

        // 内部処理用データをreadonly にする
        List<T> copiedItems = List.copyOf(items);
        // 渡された件数はmaxConcurrencyより小さいなら、maxConcurrency分の並行処理数は意味ないから
        int partitionCount = Math.min(copiedItems.size(), maxConcurrency);
        List<List<T>> partitions = partitionEvenly(copiedItems, partitionCount);

        // 異常出たら後ろの処理スレッドを出しても意味ないので、異常発生したかどうかを管理。
        // 最初に検知した失敗をthread-safeに共有するために AtomicReference を使う
        AtomicReference<Throwable> firstFailure = new AtomicReference<>();
        // executorの中、先に完成したタスクを処理するようにExecutorCompletionServiceを使う
        ExecutorCompletionService<Integer> completionService = new ExecutorCompletionService<>(executor);
        // タスクの非同期計算の結果を表す「引換券」的な役割を持つ
        List<Future<Integer>> submittedFutures = new ArrayList<>(partitions.size());

        int submittedCount = 0;
        List<Throwable> failures = new ArrayList<>();

        for (List<T> partition : partitions) {
            // 他スレッドで異常発生されたら即中止
            if (firstFailure.get() != null) {
                break;
            }

            // 空いているスレッドを請求（Permitを求める）
            acquireGlobalPermitOrFail(submittedFutures);

            // 他スレッドで異常発生されたら即中止
            if (firstFailure.get() != null) {
                // 取ったPermitを解放
                globalSemaphore.release();
                break;
            }

            // 実際のタスクを発行
            try {
                Future<Integer> future = completionService.submit(() -> {
                    try {
                        return writeOnePartition(partition, partitionWriter, firstFailure);
                    } finally {
                        // Permitを必ず解放
                        globalSemaphore.release();
                    }
                });

                submittedFutures.add(future);
                submittedCount++;
            } catch (RejectedExecutionException e) {
                // 理論上は発生しない。executorとsemaphoreは違う状態機械なので念のため、、、
                // completionService.submitもし拒否されたらここでrelease
                globalSemaphore.release();
                failures.add(e);
                firstFailure.compareAndSet(null, e);
                cancelRemaining(submittedFutures);
                break;
            }
        }

        int totalWritten = 0;

        for (int i = 0; i < submittedCount; i++) {
            try {
                Future<Integer> completed = completionService.take();
                totalWritten += completed.get();
            } catch (InterruptedException e) {
                cancelRemaining(submittedFutures);
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while waiting for parallel writer tasks", e);
            } catch (CancellationException e) {
                if (firstFailure.get() == null) {
                    failures.add(new RuntimeException("A partition future was cancelled unexpectedly", e));
                    firstFailure.compareAndSet(null, e);
                    cancelRemaining(submittedFutures);
                }
            } catch (ExecutionException e) {
                Throwable cause = unwrap(e.getCause());
                failures.add(cause);
                firstFailure.compareAndSet(null, cause);
                cancelRemaining(submittedFutures);
            }
        }

        if (!failures.isEmpty()) {
            throw buildAggregatedException(failures,  submittedCount);
        }

        return totalWritten;
    }

    // すべての異常を組み込む
    private RuntimeException buildAggregatedException(List<Throwable> failures, int submittedCount) {
        Throwable primary = failures.getFirst();
        RuntimeException exception = new RuntimeException(
                "Parallel chunk write failed. submittedPartitionCount=" + submittedCount
                + ", failureCount=" +  failures.size(), primary
        );
        for (int i = 1; i < failures.size(); i++) {
            Throwable extra = failures.get(i);
            if (extra != primary) {
                exception.addSuppressed(extra);
            }
        }
        return exception;
    }

    private <T> int writeOnePartition(
            List<T> partition,
            Consumer<List<T>> partitionWriter,
            AtomicReference<Throwable> firstFailure
    ) {
        // 他スレッドで異常発生されたら即中止
        if (firstFailure.get() != null) {
            return 0;
        }

        // 一回の実行で新しいトランザクションを作る
        TransactionTemplate tx = new TransactionTemplate(platformTransactionManager);
        tx.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);

        try {
            Integer result = tx.execute(status -> {
                if (Thread.currentThread().isInterrupted()) {
                    status.setRollbackOnly();
                    throw new RuntimeException("Writer partition thread was interrupted before execution");
                }

                if (firstFailure.get() != null) {
                    status.setRollbackOnly();
                    return 0;
                }

                partitionWriter.accept(partition);

                if (Thread.currentThread().isInterrupted()) {
                    status.setRollbackOnly();
                    throw new RuntimeException("Writer partition thread was interrupted during execution");
                }

                if (firstFailure.get() != null) {
                    status.setRollbackOnly();
                    return 0;
                }

                return partition.size();
            });

            return result == null ? 0 : result;
        } catch (RuntimeException e) {
            firstFailure.compareAndSet(null, unwrap(e));
            throw e;
        }
    }

    // ExecutionExceptionとCompletionExceptionを剥がして、本来の異常原因を見やすくする
    private Throwable unwrap(Throwable throwable) {
        Throwable current = throwable;
        while (current != null) {
            if ((current instanceof ExecutionException || current instanceof CompletionException)
                    && current.getCause() != null ) {
                current = current.getCause();
                continue;
            }
            return current;
        }
        return throwable;
    }

    // 利用可能なスレッドがある(permit)まで待つ
    private void acquireGlobalPermitOrFail(List<? extends Future<?>> submittedFutures) {
        try {
            // 利用可能なスレッドを取る
            globalSemaphore.acquire();
        } catch (InterruptedException e) {
            // 中断されたら残り分のcancelを試みる
            cancelRemaining(submittedFutures);
            // restore a thread's interrupted status
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while waiting for a parallel writer slot", e);
        }
    }

    // 中断されたら残り分のcancelを試みる
    private void cancelRemaining(List<? extends Future<?>> futures) {
        for (Future<?> future : futures) {
            if (!future.isDone()) {
                future.cancel(true);
            }
        }
    }

    // スレッド毎に処理件数を分ける
    private <T> List<List<T>> partitionEvenly(List<T> items, int partitionCount) {
        List<List<T>> partitions = new ArrayList<>(partitionCount);

        int total =  items.size();
        int baseSize = total / partitionCount;
        // あまり
        int remainder = total % partitionCount;

        int from = 0;
        for (int i = 0; i < partitionCount; i++) {
            // あまりの数を各スレッド処理分に均等に入れる
            int size = baseSize + (i < remainder ? 1 : 0);
            int to = from + size;
            partitions.add(items.subList(from, to));
            from = to;
        }
        return partitions;
    }
}
