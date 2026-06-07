package com.atlassystems.missioncontrol.config;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;
import java.util.concurrent.*;
@Configuration @EnableAsync
public class AsyncConfig {
    @Bean("observerTaskExecutor")
    public Executor observerTaskExecutor() {
        ThreadPoolExecutor ex = new ThreadPoolExecutor(
            4, 16, 60L, TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(1000),
            r -> new Thread(r, "observer-" + r.hashCode()),
            new ThreadPoolExecutor.CallerRunsPolicy()
        );
        ex.allowCoreThreadTimeOut(true);
        return ex;
    }
}
