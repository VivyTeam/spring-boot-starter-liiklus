package io.vivy.liiklus.multiple;

import com.github.bsideup.liiklus.protocol.ReceiveReply;
import io.vivy.liiklus.common.Liiklus;
import io.vivy.liiklus.consumer.LiiklusConsumer;
import io.vivy.liiklus.producer.LiiklusProducer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.publisher.Mono;

@Configuration
public class GlobalTopicConfiguration {

    @Bean(destroyMethod = "close")
    GlobalConsumer globalConsumer() {
        return new GlobalConsumer();
    }

    @Bean
    GlobalProducer globalPublisher() {
        return new GlobalProducer();
    }

    @Liiklus("global")
    public class GlobalProducer extends LiiklusProducer {
    }

    @Liiklus("global")
    public class GlobalConsumer extends LiiklusConsumer {

        @Override
        public Mono<Void> consume(int partition, ReceiveReply.Record record) {
            return Mono.empty();
        }
    }
}
