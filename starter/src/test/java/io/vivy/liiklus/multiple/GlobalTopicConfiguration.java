package io.vivy.liiklus.multiple;

import com.github.bsideup.liiklus.protocol.ReceiveReply;
import io.vivy.liiklus.common.Liiklus;
import io.vivy.liiklus.consumer.LiiklusConsumer;
import io.vivy.liiklus.publisher.LiiklusPublisher;
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
    GlobalPublisher globalPublisher() {
        return new GlobalPublisher();
    }

    @Liiklus(prefix = "global")
    public class GlobalPublisher extends LiiklusPublisher {
    }

    @Liiklus(prefix = "global")
    public class GlobalConsumer extends LiiklusConsumer {

        @Override
        public Mono<Void> consume(int partition, ReceiveReply.Record record) {
            return Mono.empty();
        }
    }
}
