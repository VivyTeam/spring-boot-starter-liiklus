package io.vivy.liiklus;

import com.github.bsideup.liiklus.LiiklusClient;
import io.vivy.liiklus.common.LiiklusUtils;
import io.vivy.liiklus.consumer.LiiklusConsumer;
import io.vivy.liiklus.consumer.LiiklusConsumerProperties;
import io.vivy.liiklus.publisher.LiiklusPublisher;
import io.vivy.liiklus.publisher.LiiklusPublisherProperties;
import io.vivy.liiklus.support.LiiklusClientFactory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.validation.Validator;
import reactor.core.scheduler.Schedulers;

import java.time.Clock;
import java.util.List;

@Configuration
@EnableConfigurationProperties(LiiklusProperties.class)
@ConditionalOnClass({LiiklusClient.class})
@Slf4j
public class LiiklusAutoConfiguration {

    @Autowired
    LiiklusProperties properties;

    @Autowired
    Environment environment;

    @Bean
    @ConditionalOnMissingBean(Clock.class)
    Clock clock() {
        return Clock.systemUTC();
    }

    @Bean
    LiiklusClient liiklusClient(Clock clock) {
        return LiiklusClientFactory.create(properties, clock);
    }

    @Bean
    public LiiklusComponentFactory liiklusComponentFactory(LiiklusClient liiklusClient) {
        var ackScheduler = Schedulers.newSingle("ack");
        var readScheduler = Schedulers.newParallel("liiklus");
        return new LiiklusComponentFactory(liiklusClient, properties.getAckInterval(), ackScheduler, readScheduler);
    }

    @Bean
    public static Validator configurationPropertiesValidator() {
        return new LiiklusProperties.LiiklusPropertiesValidator();
    }

    @Bean
    public List<LiiklusPublisher> liiklusPublisher(LiiklusClient liiklusClient, List<LiiklusPublisher> liiklusPublishers) {
        liiklusPublishers.forEach(publisher -> {
                    var prefix = LiiklusUtils.getLiiklusPrefix(publisher);
                    var publisherProperties = LiiklusPublisherProperties.create(environment, prefix);
                    publisher.init(liiklusClient, publisherProperties.getTopic());
                }
        );
        return liiklusPublishers;
    }

    @Bean
    public List<LiiklusConsumer> liiklusConsumers(LiiklusComponentFactory liiklusComponentFactory, List<LiiklusConsumer> liiklusConsumers) {
        liiklusConsumers.forEach(consumer -> {
                    var prefix = LiiklusUtils.getLiiklusPrefix(consumer);

                    var consumerProperties = LiiklusConsumerProperties.create(environment, prefix);
                    var consumerLoop = liiklusComponentFactory.createConsumer(
                            consumerProperties.getTopic(),
                            consumerProperties.getGroupName(),
                            consumerProperties.getGroupVersion(),
                            consumer);
                    consumerLoop.run();
                    consumer.init(consumerLoop);
                }
        );
        return liiklusConsumers;
    }
}