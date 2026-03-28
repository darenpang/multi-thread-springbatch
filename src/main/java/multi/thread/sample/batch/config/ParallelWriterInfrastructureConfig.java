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
        // 基本となるスレッド数。タスク到着時に必要に応じて生成される
        executor.setCorePoolSize(parallelWriterProperties.getThreadPoolSize());
        // 最大スレッド数
        executor.setMaxPoolSize(parallelWriterProperties.getThreadPoolSize());
        // executor 内では待ち行列を持たない。実際の待ちは外のSemaphoreでコントロール
        executor.setQueueCapacity(0);
        // shutdown 時に、すでに受け付けたタスクの完了を待つ
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.initialize();
        return executor;
    }
}
