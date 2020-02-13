package io.vivy.liiklus;

import com.github.bsideup.liiklus.LiiklusClient;
import com.github.bsideup.liiklus.protocol.ReceiveReply;
import io.vivy.liiklus.common.LiiklusUtils;
import io.vivy.liiklus.consumer.LiiklusConsumer;
import io.vivy.liiklus.consumer.LiiklusConsumerLoop;
import io.vivy.liiklus.consumer.LiiklusConsumerProperties;
import io.vivy.liiklus.producer.LiiklusProducer;
import io.vivy.liiklus.producer.LiiklusProducerProperties;
import io.vivy.liiklus.support.LiiklusClientFactory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.validation.Validator;
import reactor.core.publisher.Mono;

import java.time.Clock;
import java.util.List;

@Configuration
@EnableConfigurationProperties(LiiklusProperties.class)
@ConditionalOnClass({LiiklusClient.class})
@Slf4j
public class LiiklusAutoConfiguration {

    @Autowired
    LiiklusProperties properties;

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
    @ConditionalOnProperty(prefix = "liiklus", name = "topic")
    LiiklusPublisher liiklusPublisher(LiiklusClient liiklusClient) {
        return new LiiklusPublisher(properties.getTopic(), liiklusClient);
    }

    @Bean(destroyMethod = "close")
    @ConditionalOnProperty(prefix = "liiklus", name = {"topic", "groupName"})
    @ConditionalOnBean(PartitionAwareProcessor.class)
    LiiklusConsumerLoop liiklusConsumerLoop(LiiklusComponentFactory liiklusComponentFactory, PartitionAwareProcessor partitionAwareProcessor) {
        var consumer = new LiiklusConsumer() {
            @Override
            public Mono<Void> consume(int partition, ReceiveReply.Record record) {
                return partitionAwareProcessor.apply(partition, record);
            }
        };
        return createConsumerLoop(liiklusComponentFactory, consumer);
    }

    @Bean(destroyMethod = "close")
    @ConditionalOnProperty(prefix = "liiklus", name = {"topic", "groupName"})
    @ConditionalOnBean(RecordProcessor.class)
    @ConditionalOnMissingBean(PartitionAwareProcessor.class)
    LiiklusConsumerLoop liiklusConsumerLoop(LiiklusComponentFactory liiklusComponentFactory, RecordProcessor recordProcessor) {
        var consumer = new LiiklusConsumer() {
            @Override
            public Mono<Void> consume(int partition, ReceiveReply.Record record) {
                return recordProcessor.apply(record);
            }
        };
        return createConsumerLoop(liiklusComponentFactory, consumer);
    }

    @Deprecated
    private LiiklusConsumerLoop createConsumerLoop(LiiklusComponentFactory liiklusComponentFactory, LiiklusConsumer liiklusConsumer) {
        LiiklusConsumerLoop consumerLoop = liiklusComponentFactory.createConsumer(
                properties.getTopic(),
                properties.getGroupName(),
                properties.getGroupVersion(),
                liiklusConsumer
        );
        consumerLoop.run();
        return consumerLoop;
    }

    @Bean
    public LiiklusComponentFactory liiklusComponentFactory(LiiklusClient liiklusClient) {
        return new LiiklusComponentFactory(liiklusClient, properties.getAckInterval());
    }

    @Bean
    public static Validator configurationPropertiesValidator() {
        return new LiiklusProperties.LiiklusPropertiesValidator();
    }

    @Bean
    public List<LiiklusProducer> liiklusProducers(LiiklusClient liiklusClient, List<LiiklusProducer> liiklusProducers) {
        liiklusProducers.forEach(publisher -> {
                    var prefix = LiiklusUtils.getLiiklusPrefix(publisher);
                    var publisherProperties = LiiklusProducerProperties.create(properties, prefix);
                    publisher.init(liiklusClient, publisherProperties.getTopic());
                }
        );
        return liiklusProducers;
    }

    @Bean
    public List<LiiklusConsumer> liiklusConsumers(LiiklusComponentFactory liiklusComponentFactory, List<LiiklusConsumer> liiklusConsumers) {
        liiklusConsumers.forEach(consumer -> {
                    var prefix = LiiklusUtils.getLiiklusPrefix(consumer);

                    var consumerProperties = LiiklusConsumerProperties.create(properties, prefix);
                    LiiklusConsumerLoop consumerLoop = liiklusComponentFactory.createConsumer(
                            consumerProperties.getTopic(),
                            consumerProperties.getGroupName(),
                            consumerProperties.getGroupVersion(),
                            consumer
                    );
                    consumerLoop.run();
                    consumer.init(consumerLoop);
                }
        );
        return liiklusConsumers;
    }
}