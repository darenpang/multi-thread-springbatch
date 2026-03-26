package multi.thread.sample.batch.config;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app.batch.user-import")
public record UserImportJobProperties(
		String filePath,
		int chunkSize,
		Charset fileEncoding
) {

	public UserImportJobProperties {
		filePath = Objects.requireNonNull(filePath, "app.batch.user-import.file-path is required");
		if (chunkSize < 1) {
			throw new IllegalArgumentException("app.batch.user-import.chunk-size must be greater than 0");
		}
		fileEncoding = Objects.requireNonNullElse(fileEncoding, StandardCharsets.UTF_8);
	}
}
