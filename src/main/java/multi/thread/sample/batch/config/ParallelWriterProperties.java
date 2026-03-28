package multi.thread.sample.batch.config;

import jakarta.validation.constraints.Positive;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Getter
@Setter
@Validated
@ConfigurationProperties(prefix = "app.batch.parallel-writer")
public class ParallelWriterProperties {
    @Positive(message = "pool-size must > 0")
    private int poolSize;
}
