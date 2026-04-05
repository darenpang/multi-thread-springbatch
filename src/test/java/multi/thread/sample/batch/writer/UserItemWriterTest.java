package multi.thread.sample.batch.writer;

import multi.thread.sample.batch.domain.UserBatchItem;
import multi.thread.sample.batch.helper.ParallelChunkWriteExecutor;
import multi.thread.sample.infrastructure.mybatis.UserMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.batch.item.Chunk;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.LocalDate;
import java.util.List;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class UserItemWriterTest {

    @Mock
    private UserMapper userMapper;

    @Mock
    private ParallelChunkWriteExecutor parallelChunkWriteExecutor;

    @InjectMocks
    private UserItemWriter userItemWriter;

    @Captor
    private ArgumentCaptor<Consumer<List<UserBatchItem>>> partitionWriterCaptor;

    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(userItemWriter, "maxConcurrency", 3);
    }

    @Test
    @DisplayName("itemsが空")
    void shouldReturnImmediatelyWhenChunkIsEmpty() {
        Chunk<UserBatchItem> chunk = new Chunk<>(List.of());

        userItemWriter.write(chunk);

        verifyNoInteractions(parallelChunkWriteExecutor, userMapper);
    }

    @Test
    @DisplayName("itemsとmaxConcurrencyは正しく渡されている")
    void shouldDelegateItemsAndMaxConcurrencyToExecutor() {
        UserBatchItem item1 = item("", 1, "", "", LocalDate.now());
        UserBatchItem item2 = item("", 2, "", "", LocalDate.now());
        Chunk<UserBatchItem> chunk = new Chunk<>(List.of(item1, item2));

        when(parallelChunkWriteExecutor.execute(any(), any(Integer.class), any())).thenReturn(2);

        userItemWriter.write(chunk);

        verify(parallelChunkWriteExecutor).execute(eq(chunk.getItems()), eq(3), partitionWriterCaptor.capture());
    }

    @Test
    @DisplayName("userMapper.insertUserが正しく呼ばれる")
    void shouldWriteEachItemViaUserMapperThroughCapturedPartitionWriter() {
        UserBatchItem item1 = item("", 1, "", "", LocalDate.now());
        UserBatchItem item2 = item("", 2, "", "", LocalDate.now());
        Chunk<UserBatchItem> chunk = new Chunk<>(List.of(item1, item2));

        when(parallelChunkWriteExecutor.execute(any(), any(Integer.class), any())).thenReturn(2);

        userItemWriter.write(chunk);

        verify(parallelChunkWriteExecutor).execute(eq(chunk.getItems()), eq(3), partitionWriterCaptor.capture());

        Consumer<List<UserBatchItem>> consumer = partitionWriterCaptor.getValue();
        consumer.accept(List.of(item1, item2));

        verify(userMapper).insertUser(item1);
        verify(userMapper).insertUser(item2);
    }

    @Test
    @DisplayName("中断発生")
    void shouldThrowWhenCurrentThreadIsInterruptedBeforeWritingPartition() {
        UserBatchItem item1 = item("", 1, "", "", LocalDate.now());
        UserBatchItem item2 = item("", 2, "", "", LocalDate.now());
        Chunk<UserBatchItem> chunk = new Chunk<>(List.of(item1, item2));

        when(parallelChunkWriteExecutor.execute(any(), any(Integer.class), any())).thenReturn(2);

        userItemWriter.write(chunk);

        verify(parallelChunkWriteExecutor).execute(eq(chunk.getItems()), eq(3), partitionWriterCaptor.capture());

        Consumer<List<UserBatchItem>> consumer = partitionWriterCaptor.getValue();

        Thread.currentThread().interrupt();
        try {
            assertThatThrownBy(() -> consumer.accept(List.of(item1, item2)))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Writer partition thread was interrupted");
            verify(userMapper, never()).insertUser(any());
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    @DisplayName("executorの異常は外に出す")
    void shouldPropagateExceptionFromExecutor() {
        UserBatchItem item1 = item("", 1, "", "", LocalDate.now());
        Chunk<UserBatchItem> chunk = new Chunk<>(List.of(item1));

        when(parallelChunkWriteExecutor.execute(any(), any(Integer.class), any())).thenThrow(new RuntimeException("executor boom"));

        assertThatThrownBy(() -> userItemWriter.write(chunk))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("executor boom");
    }

    // =======helper

    private UserBatchItem item(
            String btn,
            Integer koza,
            String name,
            String kbn,
            LocalDate ymd
    ) {
        return UserBatchItem.builder()
                .btn(btn)
                .koza(koza)
                .name(name)
                .kbn(kbn)
                .ymd(ymd)
                .build();
    }
}