package multi.thread.sample.batch.config;

import java.nio.file.Path;
import multi.thread.sample.batch.csv.UserCsvRecord;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;

@Configuration
public class UserImportReaderConfig {

	@Bean
	@StepScope
	FlatFileItemReader<UserCsvRecord> userCsvReader(UserImportJobProperties properties) {
		return new FlatFileItemReaderBuilder<UserCsvRecord>()
				.name("userCsvReader")
				.resource(new FileSystemResource(Path.of(properties.filePath())))
				.encoding(properties.fileEncoding().name())
				.linesToSkip(1)
				.delimited()
				.names("btn", "koza", "name", "kbn", "ymd")
				.fieldSetMapper(fieldSet -> new UserCsvRecord(
						fieldSet.readString("btn"),
						fieldSet.readInt("koza"),
						fieldSet.readString("name"),
						fieldSet.readString("kbn"),
						fieldSet.readString("ymd")
				))
				.build();
	}
}
