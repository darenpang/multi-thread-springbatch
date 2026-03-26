package multi.thread.sample.batch.writer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import multi.thread.sample.batch.domain.UserBatchItem;
import multi.thread.sample.infrastructure.mybatis.UserMapper;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class UserItemWriter implements ItemWriter<UserBatchItem> {

	private final UserMapper userMapper;

	@Override
	public void write(Chunk<? extends UserBatchItem> chunk) {
		if (chunk.isEmpty()) {
			return;
		}

		var users = chunk.getItems().stream()
				.map(UserBatchItem.class::cast)
				.toList();
		userMapper.insertUsers(users);
		log.info("inserted {} users", users.size());
	}
}
