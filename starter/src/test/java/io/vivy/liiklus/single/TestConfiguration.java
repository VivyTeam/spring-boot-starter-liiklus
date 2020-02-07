package io.vivy.liiklus.single;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class TestConfiguration {

    @Bean(destroyMethod = "close")
    public TestConsumer testConsumer() {
        return new TestConsumer();
    }

    @Bean
    public TestPublisher testPublisher() {
        return new TestPublisher();
    }
}
