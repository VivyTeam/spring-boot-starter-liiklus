package io.vivy.liiklus.multiple;

import com.github.bsideup.liiklus.protocol.ReceiveReply;
import io.vivy.liiklus.common.Liiklus;
import io.vivy.liiklus.consumer.LiiklusConsumer;
import io.vivy.liiklus.producer.LiiklusProducer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.publisher.Mono;

@Configuration
public class UserTopicConfiguration {

    @Bean(destroyMethod = "close")
    UserConsumer userConsumer() {
        return new UserConsumer();
    }

    @Bean
    UserProducer userPublisher() {
        return new UserProducer();
    }

    @Liiklus("user")
    public class UserProducer extends LiiklusProducer {
    }

    @Liiklus("user")
    public class UserConsumer extends LiiklusConsumer {

        @Override
        public Mono<Void> consume(int partition, ReceiveReply.Record record) {
            return Mono.empty();
        }
    }
}
