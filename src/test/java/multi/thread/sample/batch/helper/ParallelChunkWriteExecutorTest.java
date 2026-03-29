package multi.thread.sample.batch.helper;

import multi.thread.sample.batch.config.ParallelWriterProperties;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.ThrowableAssert.catchThrowable;

class ParallelChunkWriteExecutorTest {
    /**
     * テスト観点整理
     * * A. 入力
     * (高) A.1 maxConcurrency不正　0 -1
     * (高) A.2 items不正 empty
     * (高) A.3 正常
     * * B. Partitionの切り方
     * (高) B.1 items 10件　3並列
     * (高) B.2 items 9件　3並列
     * (高) B.3 items 2件　5並列
     * (高) B.4 items 漏れ・重複なく処理される
     * (低) B.5 items 改ざんされない（Readonly）
     * * C. 並列コントロール
     * (高) C.1 並列上限超えない
     * (高) C.2 Semaphore により待機が発生する
     * (高) C.3 maxConcurrency < thread-pool-size　の場合の並列数
     * (高) C.4 maxConcurrency >= thread-pool-size　の場合の並列数
     * * D. コミットと失敗
     * (高) D.1 全スライド成功時に execute は正常終了する
     * (高) D.2 失敗あったら異常
     * (高) D.3 失敗後は後続の新規スライドを submit しない
     * (高) D.4 失敗あったら本当のcauseがでる
     * (低) D.5 RejectedExecutionException確認
     * (中) D.6 複数異常は全部でる、primaryとsuppressedは分ける
     * * E. 中断と取消
     * (高) E.1 Permitを待つときに中断
     * (高) E.2 completionを待つときに中断
     * (高) E.3 中断後interrupted flagが復元される
     * (高) E.4 CancellationException時の動作
     * (高) E.5 cancelRemainingの動作
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
                    harness.target.execute(List.of(1, 2, 3), maxConcurrency, partition -> {}));

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

    @ParameterizedTest(name = "[A.3][B] items={0}, maxConcurrency={1}, expectedPartitionSizes={2}")
    @MethodSource("validPartition")
    void A3_B1_B2_B3_B4_shouldActNormally(
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
    }


    // ↓===== テストhelpers =====

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