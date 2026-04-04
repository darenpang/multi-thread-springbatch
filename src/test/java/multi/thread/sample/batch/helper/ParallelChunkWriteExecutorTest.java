package multi.thread.sample.batch.helper;

import multi.thread.sample.batch.config.ParallelWriterProperties;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.ThrowableAssert.catchThrowable;

class ParallelChunkWriteExecutorTest {
    /**
     * テスト観点整理
     * * A. 入力
     * (高)(済) A.1 maxConcurrency不正　0 -1
     * (高)(済) A.2 items不正 empty
     * (高)(済) A.3 正常
     * * B. Partitionの切り方
     * (高)(済) B.1 items 10件　3並列
     * (高)(済) B.2 items 9件　3並列
     * (高)(済) B.3 items 2件　5並列
     * (高)(済) B.4 items 漏れ・重複なく処理される
     * (低)(済) B.5 items 改ざんされない（Readonly）
     * * C. 並列コントロール
     * (高)(済) C.1 並列上限超えない
     * (高)(済) C.2 Semaphore により待機が発生する
     * (高)(済) C.3 maxConcurrency < thread-pool-size　の場合の並列数
     * (高)(済)  C.4 maxConcurrency >= thread-pool-size　の場合の並列数
     * * D. コミットと失敗
     * (高)(済) D.1 全スライド成功時に execute は正常終了する
     * (高)(済) D.2 失敗あったら異常
     * (高)(済) D.3 失敗後は後続の新規スライドを submit しない
     * (高)(済) D.4 失敗あったら本当のcauseがでる
     * (低)(済) D.5 RejectedExecutionException確認
     * (中)(済) D.6 複数異常は全部でる、primaryとsuppressedは分ける
     * * E. 中断と取消
     * (高)(済) E.1 Permitを待つときに中断
     * (高)(済) E.2 completionを待つときに中断
     * (高)(済) E.3 中断後interrupted flagが復元される
     * (高) E.4 CancellationException時の動作
     * (高)(済) E.5 cancelRemainingの動作
     * * F. トランザクション
     * (低) F.1 独立なトランザクションになる
     * (低) F.2 一つのトランザクション失敗になってもほかのトランザクションが成功できる
     * (低) F.3 中断時にrollbackOnlyになる
     */

    static Stream<Arguments> invalidConcurrencyProvider() {
        return Stream.of(
                Arguments.of(0),
                Arguments.of(-1)
        );
    }

    @ParameterizedTest(name = "[A.1] maxConcurrency={0}　should return IllegalArgumentException")
    @MethodSource("invalidConcurrencyProvider")
    void A1_invalidMaxConcurrency(int maxConcurrency) {
        try (Harness harness = newHarness(2)) {
            Throwable thrown = catchThrowable(() ->
                    harness.target.execute(numbers(3), maxConcurrency, partition -> {}));

            assertThat(thrown)
                    .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("maxConcurrency must be greater than 0");
        }
    }

    @Test
    @DisplayName("[A.2] itemsが空")
    void A2_itemsEmpty() {
        try (Harness harness = newHarness(2)) {
            AtomicInteger count = new AtomicInteger();

            int result = harness.target.execute(
                    List.<Integer>of(),
                    3,
                    partition -> count.incrementAndGet()
            );
            assertThat(result).isZero();
            assertThat(count.get()).isZero();
        }
    }

    static Stream<Arguments> validPartition() {
        return Stream.of(
                Arguments.of(10, 3, List.of(4, 3, 3)),
                Arguments.of(9, 3, List.of(3, 3, 3)),
                Arguments.of(2, 5, List.of(1, 1))
        );
    }

    @ParameterizedTest(name = "[A.3][B][D.1] items={0}, maxConcurrency={1}, expectedPartitionSizes={2}")
    @MethodSource("validPartition")
    void A3_B1_B2_B3_B4_D1_shouldActNormally(
            int itemCount,
            int maxConcurrency,
            List<Integer> expectedPartitionSizes
    ) {
        try (Harness harness = newHarness(4)) {
            // 処理対象を生成
            List<Integer> items = numbers(itemCount);
            // スライド毎の保存先
            ConcurrentLinkedDeque<List<Integer>> observedPartitions = new ConcurrentLinkedDeque<>();
            // 処理済みスライドの保存先
            Set<Integer> processed = ConcurrentHashMap.newKeySet();

            int result = harness.target.execute(
                    items, maxConcurrency, partition -> {
                        observedPartitions.add(List.copyOf(partition));
                        processed.addAll(partition);
                    }
            );

            assertThat(result).isEqualTo(itemCount);
            assertThat(processed).containsExactlyInAnyOrderElementsOf(items);
            assertThat(observedPartitions)
                    .extracting(List::size)
                    .containsExactlyInAnyOrderElementsOf(expectedPartitionSizes);
        }

        // D1 execute正常
        try (Harness harness = newHarness(2)) {
            Throwable thrown = catchThrowable(() ->
                    harness.target.execute(numbers(3), maxConcurrency, partition -> {}));

            assertThat(thrown)
                    .doesNotThrowAnyException();
        }
    }

    @Test
    @DisplayName("[B.5] items は改ざんされない（Readonly）")
    void B5_itemsShouldBeReadonlyAndUseSnapshot() {
        try (Harness harness = newHarness(2)) {
            List<Integer> items = new ArrayList<>(numbers(4));
            ConcurrentLinkedDeque<List<Integer>> observedPartitions = new ConcurrentLinkedDeque<>();
            ConcurrentLinkedDeque<Boolean> readonlyChecks = new ConcurrentLinkedDeque<>();
            CountDownLatch originalMutated = new CountDownLatch(1);

            int result = harness.target.execute(items, 2, partition -> {
                Throwable thrown = catchThrowable(() -> partition.add(999));
                readonlyChecks.add(thrown instanceof UnsupportedOperationException);

                if (partition.contains(1)) {
                    items.clear();
                    items.add(99);
                    items.add(100);
                    originalMutated.countDown();
                } else {
                    try {
                        boolean ok = originalMutated.await(2, TimeUnit.SECONDS);
                        if (!ok) {
                            throw new RuntimeException("test timeout while waiting original items mutation");
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    }
                }

                observedPartitions.add(List.copyOf(partition));
            });

            assertThat(result).isEqualTo(4);
            assertThat(observedPartitions)
                    .containsExactlyInAnyOrder(List.of(1, 2), List.of(3, 4));
            assertThat(readonlyChecks)
                    .hasSize(2)
                    .containsOnly(true);
            assertThat(items).containsExactly(99, 100);
        }
    }

    @Test
    @DisplayName("[C.1][C.3] maxConcurrency < thread-pool-size, 並列上限はmaxConcurrencyを超えない")
    void C1_C3_shouldNotExceedMaxConcurrencyWhenItIsLowerThanThreadPoolSize() {
        // thread-pool-sizeが4のexecutorを作る
        try (Harness harness = newHarness(4)) {
            // カウントダウン（倒计时锁存器）。
            // 指定の数字で初期化され、その数字分のcountDown()が呼ばれるまでawait()はブロックします。
            CountDownLatch started = new CountDownLatch(2);
            CountDownLatch released = new CountDownLatch(1);

            // 実行中スレッド数
            AtomicInteger inFlight = new AtomicInteger();
            // 歴史最大スレッド数
            AtomicInteger maxSeen = new AtomicInteger();

            // メインスレッドをブロックしないように、新しいサブスレッド（caller）で処理させる
            try (ExecutorService caller = Executors.newSingleThreadExecutor()) {
                try {
                    Future<Integer> future = caller.submit(() ->
                            harness.target.execute(numbers(8), 2, partition -> {
                                started.countDown();

                                int current = inFlight.incrementAndGet();
                                maxSeen.accumulateAndGet(current, Math::max);

                                try {
                                    boolean ok = released.await(5, TimeUnit.SECONDS);
                                    if (!ok) {
                                        throw new RuntimeException("test timeout while waiting release");
                                    }
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                    throw new RuntimeException(e);
                                } finally {
                                    inFlight.decrementAndGet();
                                }
                            })
                    );

                    // True：TimeOutする前CountDownLatchが0になる。
                    // False：TimeOutする前CountDownLatchが0にならない。
                    assertThat(started.await(2, TimeUnit.SECONDS)).isTrue();
                    assertThat(maxSeen.get()).isLessThanOrEqualTo(2);

                    released.countDown();

                    assertThat(future.get(2, TimeUnit.SECONDS)).isEqualTo(8);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("test interrupted", e);
                } catch (ExecutionException e) {
                    throw new RuntimeException("async execution failed in test", e);
                } catch (TimeoutException e) {
                    throw new RuntimeException("test timed out", e);
                } finally {
                    caller.shutdown();
                }
            }
        }
    }


    @Test
    @DisplayName("[C.1][C.4] maxConcurrency >= thread-pool-size, 並列上限はthread-pool-sizeを超えない")
    void C1_C4_shouldNotExceedThreadPoolSizeWhenItIsLargerThanOrEqualToThreadPoolSize() {
        // thread-pool-sizeが4のexecutorを作る
        try (Harness harness = newHarness(4)) {
            // カウントダウン（倒计时锁存器）。
            // 指定の数字で初期化され、その数字分のcountDown()が呼ばれるまでawait()はブロックします。
            CountDownLatch started = new CountDownLatch(2);
            CountDownLatch released = new CountDownLatch(1);

            // 実行中スレッド数
            AtomicInteger inFlight = new AtomicInteger();
            // 歴史最大スレッド数
            AtomicInteger maxSeen = new AtomicInteger();

            // メインスレッドをブロックしないように、新しいサブスレッド（caller）で処理させる
            // >
            try (ExecutorService caller = Executors.newSingleThreadExecutor()) {
                try {
                    Future<Integer> future = caller.submit(() ->
                            harness.target.execute(numbers(8), 8, partition -> {
                                started.countDown();

                                int current = inFlight.incrementAndGet();
                                maxSeen.accumulateAndGet(current, Math::max);

                                try {
                                    boolean ok = released.await(5, TimeUnit.SECONDS);
                                    if (!ok) {
                                        throw new RuntimeException("test timeout while waiting release");
                                    }
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                    throw new RuntimeException(e);
                                } finally {
                                    inFlight.decrementAndGet();
                                }
                            })
                    );

                    // True：TimeOutする前CountDownLatchが0になる。
                    // False：TimeOutする前CountDownLatchが0にならない。
                    assertThat(started.await(2, TimeUnit.SECONDS)).isTrue();
                    assertThat(maxSeen.get()).isLessThanOrEqualTo(4);

                    released.countDown();

                    assertThat(future.get(2, TimeUnit.SECONDS)).isEqualTo(8);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("test interrupted", e);
                } catch (ExecutionException e) {
                    throw new RuntimeException("async execution failed in test", e);
                } catch (TimeoutException e) {
                    throw new RuntimeException("test timed out", e);
                } finally {
                    caller.shutdown();
                }
            }


            // =
            try (ExecutorService caller = Executors.newSingleThreadExecutor()) {
                try {
                    Future<Integer> future = caller.submit(() ->
                            harness.target.execute(numbers(8), 4, partition -> {
                                started.countDown();

                                int current = inFlight.incrementAndGet();
                                maxSeen.accumulateAndGet(current, Math::max);

                                try {
                                    boolean ok = released.await(5, TimeUnit.SECONDS);
                                    if (!ok) {
                                        throw new RuntimeException("test timeout while waiting release");
                                    }
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                    throw new RuntimeException(e);
                                } finally {
                                    inFlight.decrementAndGet();
                                }
                            })
                    );

                    // True：TimeOutする前CountDownLatchが0になる。
                    // False：TimeOutする前CountDownLatchが0にならない。
                    assertThat(started.await(2, TimeUnit.SECONDS)).isTrue();
                    assertThat(maxSeen.get()).isLessThanOrEqualTo(4);

                    released.countDown();

                    assertThat(future.get(2, TimeUnit.SECONDS)).isEqualTo(8);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("test interrupted", e);
                } catch (ExecutionException e) {
                    throw new RuntimeException("async execution failed in test", e);
                } catch (TimeoutException e) {
                    throw new RuntimeException("test timed out", e);
                } finally {
                    caller.shutdown();
                }
            }
        }
    }

    @Test
    @DisplayName("[D.2][D.4] 失敗したら異常、causeは見える")
    void D2_D4_shouldThrowAndExposeRealCauseWhenOnePartitionFails() {
        try (Harness harness = newHarness(2)) {
            Throwable thrown = catchThrowable(() ->
                    harness.target.execute(numbers(4), 2, partition -> {
                        // 1が含まれた場合のみ異常
                        if (partition.contains(1)) {
                            throw new IllegalStateException("boom-1234567890");
                        }
                    })
            );
            assertThat(thrown.getCause())
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("boom-1234567890");
            assertThat(thrown)
                    .isInstanceOf(RuntimeException.class)
                    .hasMessageContaining("Parallel chunk write failed");
        }
    }

    @Test
    @DisplayName("[D.3] 失敗後は後続の新規スライドを submit しない")
    void D3_shouldStopSubmittingNewPartitionsAfterFirstFailure() {
        try (Harness harness = newHarness(1)) {
            AtomicInteger invokedPartitions = new AtomicInteger();

            Throwable thrown = catchThrowable(() ->
                    harness.target.execute(numbers(8), 4, partition -> {
                        invokedPartitions.incrementAndGet();
                        if (partition.contains(1)) {
                            throw new IllegalStateException("boom first partition 0987654321.");
                        }
                    }));
            assertThat(thrown).isInstanceOf(RuntimeException.class);
            assertThat(invokedPartitions.get()).isEqualTo(1);
        }
    }

    @Test
    @DisplayName("[D.5] RejectedExecutionException 確認")
    void D5_shouldWrapRejectedExecutionExceptionWhenSubmitIsRejected() {
        try (Harness harness = newHarness(1)) {
            harness.executor.destroy();

            Throwable thrown = catchThrowable(() ->
                    harness.target.execute(numbers(1), 1, partition -> {}));

            assertThat(thrown)
                    .isInstanceOf(RuntimeException.class)
                    .hasMessageContaining("Parallel chunk write failed")
                    .hasMessageContaining("submittedPartitionCount=0")
                    .hasMessageContaining("failureCount=1")
                    .hasCauseInstanceOf(RejectedExecutionException.class);
            assertThat(thrown.getCause()).isInstanceOf(RejectedExecutionException.class);
            assertThat(thrown.getSuppressed()).isEmpty();
        }
    }

    @Test
    @DisplayName("[D.6] 複数異常は全部でる、primaryとsuppressedは分ける")
    void D6_multiExceptionShouldThrow() {
        try (Harness harness = newHarness(2)) {
            CountDownLatch started = new CountDownLatch(2);
            CountDownLatch release = new CountDownLatch(1);

            // メインスレッドをブロックしないように、新しいサブスレッド（caller）で処理させる
            try (ExecutorService caller = Executors.newSingleThreadExecutor()) {
                try {
                    Future<Integer> future = caller.submit(() ->
                            harness.target.execute(numbers(4), 2, partition -> {
                                started.countDown();

                                try {
                                    // 各partitionの処理開始を待ち合わせる
                                    boolean ok = started.await(2, TimeUnit.SECONDS);
                                    if (!ok) {
                                        throw new RuntimeException("test timeout while waiting both partitions to start");
                                    }

                                    // サブスレッドのrelease指示を待つ
                                    boolean released = release.await(2, TimeUnit.SECONDS);
                                    if (!released) {
                                        throw new RuntimeException("test timed out while waiting release");
                                    }
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                    throw new RuntimeException(e);
                                }

                                if (partition.contains(1)) {
                                    throw new IllegalStateException("boom-12");
                                } else {
                                    throw new IllegalStateException("boom-34");
                                }
                            })
                    );
                    // 両方ともpartitionWriterに入っているかどうか
                    assertThat(started.await(3, TimeUnit.SECONDS)).isTrue();
                    // releaseして、異常出すロジックに入らせる
                    release.countDown();

                    Throwable thrown = catchThrowable(() -> future.get(2, TimeUnit.SECONDS));

                    // future.getで出た異常はExecutionException
                    assertThat(thrown).isInstanceOf(ExecutionException.class);
                    // その中はParallelChunkWriteExecutorで組み込んだRuntimeException
                    Throwable actual = thrown.getCause();
                    assertThat(actual).isInstanceOf(RuntimeException.class);
                    assertThat(actual.getCause()).isNotNull();

                    List<String> messages = new ArrayList<>();
                    messages.add(actual.getCause().getMessage());
                    Arrays.stream(actual.getSuppressed())
                            .map(Throwable::getMessage)
                            .forEach(messages::add);

                    assertThat(messages)
                            .isNotEmpty()
                            .containsAnyOf("boom-12", "boom-34")
                            .allMatch(msg -> msg.equals("boom-12") || msg.equals("boom-34"))
                            .doesNotHaveDuplicates();

                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                } finally {
                    caller.shutdown();
                }
            }
        }
    }

    @Test
    @DisplayName("[C.2][E.1][E.3] Permitを待つときに中断")
    void C2_E1_E3_interruptedWhileWaitingPermit() {
        try (Harness harness = newHarness(1)) {
            CountDownLatch firstStarted = new CountDownLatch(1);
            CountDownLatch releaseFirst = new CountDownLatch(1);

            try (ExecutorService blocker = Executors.newSingleThreadExecutor()) {
                Future<Integer> firstFuture = blocker.submit(() ->
                        harness.target.execute(numbers(1), 1, partition -> {
                            firstStarted.countDown();
                            try {
                                boolean ok = releaseFirst.await(5, TimeUnit.SECONDS);
                                if (!ok) {
                                    throw new RuntimeException("test timeout while waiting releaseFirst");
                                }
                            }  catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                throw new RuntimeException(e);
                            }
                        })
                );
                assertThat(firstStarted.await(2, TimeUnit.SECONDS)).isTrue();

                AtomicReference<Throwable> thrownRef = new AtomicReference<>();
                AtomicBoolean interruptedFlagRef = new AtomicBoolean(false);

                Thread waitingThread = new Thread(() -> {
                    try {
                        harness.target.execute(List.of(2, 3), 1, partition -> {});
                    } catch (Throwable t) {
                        thrownRef.set(t);
                    }  finally {
                        interruptedFlagRef.set(Thread.currentThread().isInterrupted());
                    }
                }, "permit-waiting-thread");

                waitingThread.start();
                awaitThreadState(waitingThread, Duration.ofSeconds(2), Thread.State.WAITING, Thread.State.TIMED_WAITING);

                waitingThread.interrupt();
                waitingThread.join(2000);

                assertThat(thrownRef.get()).isInstanceOf(RuntimeException.class);
                assertThat(thrownRef.get().getMessage()).contains("parallel writer slot");
                assertThat(thrownRef.get().getCause()).isInstanceOf(InterruptedException.class);
                assertThat(interruptedFlagRef.get()).isTrue();

                releaseFirst.countDown();
                firstFuture.get(2, TimeUnit.SECONDS);
            } catch  (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                throw new RuntimeException("async execution failed in test", e);
            } catch (TimeoutException e) {
                throw new RuntimeException("test timed out", e);
            }
        }
    }

    @Test
    @DisplayName("[E.2][E.3] completionを待つときに中断")
    void E2_E3_interruptedWhileWaitingPermit() {
        try (Harness harness = newHarness(2)) {
            CountDownLatch started = new CountDownLatch(2);
            CountDownLatch release = new CountDownLatch(1);

            AtomicReference<Throwable> thrownRef = new AtomicReference<>();
            AtomicBoolean interruptedFlagRef = new AtomicBoolean(false);

            Thread callerThread = new Thread(() -> {
                try {
                    harness.target.execute(numbers(4), 2, partition -> {
                        started.countDown();
                        try {
                            boolean ok = release.await(5, TimeUnit.SECONDS);
                            if (!ok) {
                                throw new RuntimeException("test timeout while waiting release");
                            }
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new RuntimeException(e);
                        }
                    });
                } catch (Throwable t) {
                    thrownRef.set(t);
                } finally {
                    interruptedFlagRef.set(Thread.currentThread().isInterrupted());
                }
            }, "completion-waiting-thread");

            callerThread.start();
            assertThat(started.await(2, TimeUnit.SECONDS)).isTrue();
            awaitThreadState(callerThread, Duration.ofSeconds(2), Thread.State.WAITING, Thread.State.TIMED_WAITING);

            callerThread.interrupt();
            callerThread.join(2000);

            release.countDown();

            assertThat(thrownRef.get()).isInstanceOf(RuntimeException.class);
            assertThat(thrownRef.get().getMessage()).contains("parallel writer tasks");
            assertThat(thrownRef.get().getCause()).isInstanceOf(InterruptedException.class);
            assertThat(interruptedFlagRef.get()).isTrue();
        } catch  (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    @Test
    @DisplayName("[E.5] cancelRemaining は未完了のpartition を cancel する")
    void E5_shouldCancelRemainingPartitionsAfterFailure() {
        try (Harness harness = newHarness(2)) {
            CountDownLatch started = new CountDownLatch(2);
            CountDownLatch canceledPartitionInterrupted = new CountDownLatch(1);
            CountDownLatch blocker = new CountDownLatch(1);

            AtomicBoolean cancelObserved = new AtomicBoolean(false);

            try (ExecutorService caller = Executors.newSingleThreadExecutor()) {
                Future<Integer> future = caller.submit(() ->
                        harness.target.execute(numbers(4), 2, partition -> {
                            started.countDown();

                            try {
                                boolean ok = started.await(2, TimeUnit.SECONDS);
                                if (!ok) {
                                    throw new RuntimeException("test timeout while waiting both partitions to start");
                                }

                                if (partition.contains(1)) {
                                    throw new IllegalStateException("boom-cancel-remaining");
                                }

                                boolean released = blocker.await(5, TimeUnit.SECONDS);
                                if (!released) {
                                    throw new RuntimeException("test timeout while waiting cancellation");
                                }
                            } catch (InterruptedException e) {
                                cancelObserved.set(true);
                                canceledPartitionInterrupted.countDown();
                                Thread.currentThread().interrupt();
                                throw new RuntimeException(e);
                            }
                        })
                );

                assertThat(started.await(2, TimeUnit.SECONDS)).isTrue();
                assertThat(canceledPartitionInterrupted.await(5, TimeUnit.SECONDS)).isTrue();

                Throwable thrown = catchThrowable(() -> future.get(2, TimeUnit.SECONDS));

                assertThat(thrown).isInstanceOf(ExecutionException.class);
                assertThat(thrown.getCause())
                        .isInstanceOf(RuntimeException.class)
                        .hasMessageContaining("Parallel chunk write failed");
                assertThat(thrown.getCause().getCause())
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessageContaining("boom-cancel-remaining");
                assertThat(cancelObserved.get()).isTrue();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
    }


    // ↓===== テストhelpers =====

    private static void awaitThreadState(
            Thread thread,
            Duration timeout,
            Thread.State... expectedStates
    ) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        List<Thread.State> expected = Arrays.asList(expectedStates);

        while (System.nanoTime() < deadline) {
            if (expected.contains(thread.getState())) {
                return;
            }
            Thread.sleep(10);
        }
        throw new AssertionError("thread did not enter expected states " + expected + ", actual=" +  thread.getState());
    }

    // 　1~count分のListを一瞬で生成
    private static List<Integer> numbers(int count) {
        return IntStream.rangeClosed(1, count).boxed().toList();
    }

    private static Harness newHarness(int poolSize) {
        ParallelWriterProperties properties = new  ParallelWriterProperties();
        properties.setThreadPoolSize(poolSize);

        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setThreadNamePrefix("test-parallel-writer-");
        executor.setCorePoolSize(poolSize);
        executor.setMaxPoolSize(poolSize);
        executor.setQueueCapacity(0);
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.initialize();

        ParallelChunkWriteExecutor target = new ParallelChunkWriteExecutor(
                executor,
                new ResourcelessTransactionManager(),
                properties
        );

        return new Harness(target,  executor);
    }

    private record Harness (
            ParallelChunkWriteExecutor target,
            ThreadPoolTaskExecutor executor
    ) implements AutoCloseable {
        @Override
        public void close() {
            executor.destroy();
        }
    }
}
