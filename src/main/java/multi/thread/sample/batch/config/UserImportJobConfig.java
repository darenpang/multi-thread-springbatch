package multi.thread.sample.batch.config;

import multi.thread.sample.batch.csv.UserCsvRecord;
import multi.thread.sample.batch.domain.UserBatchItem;
import multi.thread.sample.batch.processor.UserItemProcessor;
import multi.thread.sample.batch.tasklet.TruncateUsersTasklet;
import multi.thread.sample.batch.writer.UserItemWriter;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
public class UserImportJobConfig {

	@Bean
	Job userImportJob(
			JobRepository jobRepository,
			Step truncateUsersStep,
			Step importUsersStep
	) {
		return new JobBuilder("userImportJob", jobRepository)
				.incrementer(new RunIdIncrementer())
				.start(truncateUsersStep)
				.next(importUsersStep)
				.build();
	}

	@Bean
	Step truncateUsersStep(
			JobRepository jobRepository,
			PlatformTransactionManager transactionManager,
			TruncateUsersTasklet truncateUsersTasklet
	) {
		return new StepBuilder("truncateUsersStep", jobRepository)
				.tasklet(truncateUsersTasklet, transactionManager)
				.build();
	}

	@Bean
	Step importUsersStep(
			JobRepository jobRepository,
			PlatformTransactionManager transactionManager,
			UserImportJobProperties properties,
			FlatFileItemReader<UserCsvRecord> userCsvReader,
			UserItemProcessor userItemProcessor,
			UserItemWriter userItemWriter
	) {
		return new StepBuilder("importUsersStep", jobRepository)
				.<UserCsvRecord, UserBatchItem>chunk(properties.chunkSize(), transactionManager)
				.reader(userCsvReader)
				.processor(userItemProcessor)
				.writer(userItemWriter)
				.build();
	}
}
