package multi.thread.sample;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.ConfigurableApplicationContext;

@SpringBootApplication
@ConfigurationPropertiesScan
public class SampleApplication {

	static void main(String[] args) {
		ConfigurableApplicationContext applicationContext = SpringApplication.run(SampleApplication.class, args);
		int exitCode = SpringApplication.exit(applicationContext);
		System.exit(exitCode);
	}

}
