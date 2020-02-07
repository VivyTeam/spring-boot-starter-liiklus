package io.vivy.liiklus.multiple;

import com.github.bsideup.liiklus.protocol.ReceiveReply;
import io.vivy.liiklus.common.Liiklus;
import io.vivy.liiklus.consumer.LiiklusConsumer;
import io.vivy.liiklus.publisher.LiiklusPublisher;
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
    UserPublisher userPublisher() {
        return new UserPublisher();
    }

    @Liiklus(prefix = "user")
    public class UserPublisher extends LiiklusPublisher {
    }

    @Liiklus(prefix = "user")
    public class UserConsumer extends LiiklusConsumer {

        @Override
        public Mono<Void> consume(int partition, ReceiveReply.Record record) {
            return Mono.empty();
        }
    }
}
