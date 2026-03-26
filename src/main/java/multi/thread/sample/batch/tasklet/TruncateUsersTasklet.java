package multi.thread.sample.batch.tasklet;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import multi.thread.sample.infrastructure.mybatis.UserMapper;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class TruncateUsersTasklet implements Tasklet {

	private final UserMapper userMapper;

	@Override
	public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) {
		userMapper.truncateUsers();
		log.info("users table truncated");
		return RepeatStatus.FINISHED;
	}
}
