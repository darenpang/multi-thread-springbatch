package multi.thread.sample.batch.helper;

import multi.thread.sample.batch.config.ParallelWriterProperties;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.ThrowableAssert.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ParallelChunkWriteExecutorAdditionalCoverageTest {
    /**
     * テスト観点整理
     * * G. 内部レア分岐と補助ロジック
     * (中)(済) G.1 writeOnePartition冒頭でfirstFailure済みなら0件
     * (中)(済) G.2 writeOnePartition実行前interruptならrollbackで異常
     * (中)(済) G.3 writeOnePartition実行前にfirstFailureが立ったらrollbackで0件
     * (中)(済) G.4 writeOnePartition実行後interruptならrollbackで異常
     * (中)(済) G.5 writeOnePartition実行後にfirstFailureが立ったらrollbackで0件
     * (低)(済) G.6 TransactionTemplateがnullを返したら0件
     * (低)(済) G.7 unwrap/buildAggregatedExceptionの補助分岐
     */

    @Test
    @DisplayName("[G.1] writeOnePartition冒頭でfirstFailure済みなら0件")
    void G1_shouldReturnZeroWhenFirstFailureAlreadyExists() {
        try (Harness harness = newHarness(1)) {
            AtomicReference<Throwable> firstFailure = new AtomicReference<>(
                    new IllegalStateException("already-failed"));
            AtomicInteger invokedPartitions = new AtomicInteger();

            int result = invokeWriteOnePartition(
                    harness.target,
                    List.of(1),
                    partition -> invokedPartitions.incrementAndGet(),
                    firstFailure
            );

            assertThat(result).isZero();
            assertThat(invokedPartitions.get()).isZero();
        }
    }

    @Test
    @DisplayName("[G.2] writeOnePartition実行前interruptならrollbackで異常")
    void G2_shouldThrowWhenInterruptedBeforePartitionExecution() {
        try (Harness harness = newHarness(1)) {
            AtomicReference<Throwable> firstFailure = new AtomicReference<>();

            Thread.currentThread().interrupt();
            try {
                Throwable thrown = catchThrowable(() ->
                        invokeWriteOnePartition(harness.target, List.of(1), partition -> {}, firstFailure));

                assertThat(thrown)
                        .isInstanceOf(RuntimeException.class)
                        .hasMessageContaining("Writer partition thread was interrupted before execution");
                assertThat(firstFailure.get())
                        .isInstanceOf(RuntimeException.class)
                        .hasMessageContaining("Writer partition thread was interrupted before execution");
            } finally {
                Thread.interrupted();
            }
        }
    }

    @Test
    @DisplayName("[G.3] writeOnePartition実行前にfirstFailureが立ったらrollbackで0件")
    void G3_shouldReturnZeroWhenFailureIsDetectedBeforeWriterExecution() {
        try (Harness harness = newHarness(1)) {
            AtomicReference<Throwable> firstFailure = new AtomicReference<>();
            AtomicInteger invokedPartitions = new AtomicInteger();
            AtomicReference<TransactionStatus> statusRef = new AtomicReference<>();

            try (MockedConstruction<TransactionTemplate> mocked = mockConstruction(
                    TransactionTemplate.class,
                    (mock, context) -> when(mock.execute(any())).thenAnswer(invocation -> {
                        firstFailure.set(new IllegalStateException("pre-writer-failure"));

                        @SuppressWarnings("unchecked")
                        TransactionCallback<Integer> callback = invocation.getArgument(0);
                        TransactionStatus status = mock(TransactionStatus.class);
                        statusRef.set(status);
                        return callback.doInTransaction(status);
                    }))) {
                int result = invokeWriteOnePartition(
                        harness.target,
                        List.of(1),
                        partition -> invokedPartitions.incrementAndGet(),
                        firstFailure
                );

                assertThat(mocked.constructed()).hasSize(1);
                assertThat(result).isZero();
                assertThat(invokedPartitions.get()).isZero();
                verify(statusRef.get()).setRollbackOnly();
            }
        }
    }

    @Test
    @DisplayName("[G.4] writeOnePartition実行後interruptならrollbackで異常")
    void G4_shouldThrowWhenInterruptedAfterPartitionExecution() {
        try (Harness harness = newHarness(1)) {
            AtomicReference<Throwable> firstFailure = new AtomicReference<>();

            try {
                Throwable thrown = catchThrowable(() ->
                        invokeWriteOnePartition(
                                harness.target,
                                List.of(1),
                                partition -> Thread.currentThread().interrupt(),
                                firstFailure
                        ));

                assertThat(thrown)
                        .isInstanceOf(RuntimeException.class)
                        .hasMessageContaining("Writer partition thread was interrupted during execution");
                assertThat(firstFailure.get())
                        .isInstanceOf(RuntimeException.class)
                        .hasMessageContaining("Writer partition thread was interrupted during execution");
            } finally {
                Thread.interrupted();
            }
        }
    }

    @Test
    @DisplayName("[G.5] writeOnePartition実行後にfirstFailureが立ったらrollbackで0件")
    void G5_shouldReturnZeroWhenFailureIsDetectedAfterWriterExecution() {
        try (Harness harness = newHarness(1)) {
            AtomicReference<Throwable> firstFailure = new AtomicReference<>();
            AtomicInteger invokedPartitions = new AtomicInteger();

            int result = invokeWriteOnePartition(
                    harness.target,
                    List.of(1),
                    partition -> {
                        invokedPartitions.incrementAndGet();
                        firstFailure.set(new IllegalStateException("post-writer-failure"));
                    },
                    firstFailure
            );

            assertThat(result).isZero();
            assertThat(invokedPartitions.get()).isEqualTo(1);
        }
    }

    @Test
    @DisplayName("[G.6] TransactionTemplateがnullを返したら0件")
    void G6_shouldReturnZeroWhenTransactionTemplateReturnsNull() {
        try (Harness harness = newHarness(1)) {
            AtomicReference<Throwable> firstFailure = new AtomicReference<>();
            AtomicInteger invokedPartitions = new AtomicInteger();

            try (MockedConstruction<TransactionTemplate> mocked = mockConstruction(
                    TransactionTemplate.class,
                    (mock, context) -> when(mock.execute(any())).thenReturn(null))) {
                int result = invokeWriteOnePartition(
                        harness.target,
                        List.of(1),
                        partition -> invokedPartitions.incrementAndGet(),
                        firstFailure
                );

                assertThat(mocked.constructed()).hasSize(1);
                assertThat(result).isZero();
                assertThat(invokedPartitions.get()).isZero();
            }
        }
    }

    @Test
    @DisplayName("[G.7] unwrap/buildAggregatedExceptionの補助分岐")
    void G7_shouldCoverHelperBranches() {
        try (Harness harness = newHarness(1)) {
            IllegalStateException primary = new IllegalStateException("primary-helper");
            IllegalArgumentException secondary = new IllegalArgumentException("secondary-helper");
            ExecutionException noCause = new ExecutionException("no-cause-helper", null);
            AtomicReference<Throwable> recordedFailure = new AtomicReference<>();

            Throwable unwrappedCompletion = invokeUnwrap(
                    harness.target,
                    new CompletionException(primary)
            );
            Throwable unwrappedNoCause = invokeUnwrap(harness.target, noCause);
            Throwable unwrappedNull = invokeUnwrap(harness.target, null);

            RuntimeException aggregated = invokeBuildAggregatedException(
                    harness.target,
                    List.of(primary, primary),
                    2
            );
            RuntimeException aggregatedWithSuppressed = invokeBuildAggregatedException(
                    harness.target,
                    List.of(primary, secondary),
                    2
            );
            invokePrivate(
                    harness.target,
                    "recordFirstFailure",
                    new Class<?>[]{AtomicReference.class, Throwable.class, String.class},
                    recordedFailure,
                    null,
                    "helper-null-cause"
            );

            assertThat(unwrappedCompletion).isSameAs(primary);
            assertThat(unwrappedNoCause).isSameAs(noCause);
            assertThat(unwrappedNull).isNull();
            assertThat(aggregated.getCause()).isSameAs(primary);
            assertThat(aggregated.getSuppressed()).isEmpty();
            assertThat(aggregated)
                    .hasMessageContaining("submittedPartitionCount=2")
                    .hasMessageContaining("failureCount=2");
            assertThat(aggregatedWithSuppressed.getCause()).isSameAs(primary);
            assertThat(aggregatedWithSuppressed.getSuppressed())
                    .hasSize(1)
                    .containsExactly(secondary);
            assertThat(recordedFailure.get())
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("first failure cause was null");
        }
    }


    // ↓===== テストhelpers =====


    @SuppressWarnings("unchecked")
    private static <T> int invokeWriteOnePartition(
            ParallelChunkWriteExecutor target,
            List<T> partition,
            java.util.function.Consumer<List<T>> partitionWriter,
            AtomicReference<Throwable> firstFailure
    ) {
        return (int) invokePrivate(
                target,
                "writeOnePartition",
                new Class<?>[]{List.class, java.util.function.Consumer.class, AtomicReference.class},
                partition,
                partitionWriter,
                firstFailure
        );
    }

    private static Throwable invokeUnwrap(ParallelChunkWriteExecutor target, Throwable throwable) {
        return (Throwable) invokePrivate(
                target,
                "unwrap",
                new Class<?>[]{Throwable.class},
                throwable
        );
    }

    private static RuntimeException invokeBuildAggregatedException(
            ParallelChunkWriteExecutor target,
            List<Throwable> failures,
            int submittedCount
    ) {
        return (RuntimeException) invokePrivate(
                target,
                "buildAggregatedException",
                new Class<?>[]{List.class, int.class},
                failures,
                submittedCount
        );
    }

    private static Object invokePrivate(
            Object target,
            String methodName,
            Class<?>[] parameterTypes,
            Object... args
    ) {
        try {
            Method method = target.getClass().getDeclaredMethod(methodName, parameterTypes);
            method.setAccessible(true);
            return method.invoke(target, args);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            if (cause instanceof Error error) {
                throw error;
            }
            throw new RuntimeException(cause);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    private static Harness newHarness(int poolSize) {
        ParallelWriterProperties properties = new ParallelWriterProperties();
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

        return new Harness(target, executor);
    }

    private record Harness(
            ParallelChunkWriteExecutor target,
            ThreadPoolTaskExecutor executor
    ) implements AutoCloseable {
        @Override
        public void close() {
            executor.destroy();
        }
    }
}
