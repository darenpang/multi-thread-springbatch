package multi.thread.sample.batch.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@Configuration
@EnableConfigurationProperties(ParallelWriterProperties.class)
public class ParallelWriterInfrastructureConfig {

    @Bean(name = "parallelWriter")
    public ThreadPoolTaskExecutor parallelWriterExecutor(ParallelWriterProperties  parallelWriterProperties) {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        // 各スレッドの名前のprefix
        executor.setThreadNamePrefix("parallelWriter-");
        // 常駐スレッド数
        executor.setCorePoolSize(parallelWriterProperties.getPoolSize());
        // 最大スレッド数
        executor.setMaxPoolSize(parallelWriterProperties.getPoolSize());
        // キューを0にして、実際の待ちは外のSemaphoreでコントロール
        executor.setQueueCapacity(0);
        // タスク完了まで待つ
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.initialize();
        return executor;
    }
}
