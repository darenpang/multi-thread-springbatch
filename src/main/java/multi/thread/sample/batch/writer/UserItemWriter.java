package multi.thread.sample.batch.writer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import multi.thread.sample.batch.domain.UserBatchItem;
import multi.thread.sample.batch.helper.ParallelChunkWriteExecutor;
import multi.thread.sample.infrastructure.mybatis.UserMapper;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class UserItemWriter implements ItemWriter<UserBatchItem> {

	private final UserMapper userMapper;
	private final ParallelChunkWriteExecutor executor;

	@Value("${app.batch.max-concurrency}")
	private int maxConcurrency;

	@Override
	public void write(Chunk<? extends UserBatchItem> chunk) {
		if (chunk.isEmpty()) {
			return;
		}

		try {
			int written = executor.execute(
					chunk.getItems(),
					maxConcurrency,
					this::writePartition
			);
			log.info("inserted {} users (maxConcurrency={})", written, maxConcurrency);
		} catch (RuntimeException e) {
			log.error("UserItemWriter failed: itemCount={}, maxConcurrency={}", chunk.size(), maxConcurrency, e);
			throw e;
		}
	}

	private void writePartition(List<UserBatchItem> items) {
		for (UserBatchItem item : items) {
			if (Thread.currentThread().isInterrupted()) {
				throw new IllegalStateException("Writer partition thread was interrupted");
			}
			userMapper.insertUser(item);
		}
	}
}
