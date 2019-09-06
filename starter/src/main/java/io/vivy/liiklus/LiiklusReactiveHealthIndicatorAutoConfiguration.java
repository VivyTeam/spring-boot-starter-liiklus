package io.vivy.liiklus;

import com.github.bsideup.liiklus.LiiklusClient;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.actuate.autoconfigure.health.ConditionalOnEnabledHealthIndicator;
import org.springframework.boot.actuate.health.AbstractReactiveHealthIndicator;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.ReactiveHealthIndicator;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;

@Configuration
@ConditionalOnClass({LiiklusClient.class, Flux.class, AbstractReactiveHealthIndicator.class})
@ConditionalOnBean(LiiklusClient.class)
@ConditionalOnEnabledHealthIndicator("liiklus")
@RequiredArgsConstructor
public class LiiklusReactiveHealthIndicatorAutoConfiguration {

    final LiiklusProperties properties;

    @Bean
    @ConditionalOnMissingBean(name = "liiklusHealthIndicator")
    @ConditionalOnProperty(prefix = "liiklus", value = "target")
    ReactiveHealthIndicator liiklusHealthIndicator() {
        return new LiiklusReactiveHealthIndicator(
                properties.getTarget()
        );
    }

    @Bean
    @ConditionalOnMissingBean(name = "liiklusReadHealthIndicator")
    @ConditionalOnProperty(prefix = "liiklus", value = "read.uri")
    ReactiveHealthIndicator liiklusReadHealthIndicator() {
        return new LiiklusReactiveHealthIndicator(
                properties.getRead().getUri()
        );
    }

    @Bean
    @ConditionalOnMissingBean(name = "liiklusWriteHealthIndicator")
    @ConditionalOnProperty(prefix = "liiklus", value = "write.uri")
    ReactiveHealthIndicator liiklusWriteHealthIndicator() {
        return new LiiklusReactiveHealthIndicator(
                properties.getWrite().getUri()
        );
    }

    @RequiredArgsConstructor
    private static class LiiklusReactiveHealthIndicator extends AbstractReactiveHealthIndicator {
        private final URI uri;

        @Override
        protected Mono<Health> doHealthCheck(Health.Builder builder) {
            return Mono
                    .fromSupplier(() -> {
                        try (Socket socket = new Socket()) {
                            socket.connect(new InetSocketAddress(uri.getHost(), uri.getPort()), 1000);
                            return builder.up().build();
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    })
                    .retry(3)
                    .subscribeOn(Schedulers.immediate())
                    .onErrorResume(__ -> Mono.just(builder.down().build()));
        }
    }
}
