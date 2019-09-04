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
import java.util.function.Supplier;

@Configuration
@ConditionalOnClass({LiiklusClient.class, Flux.class, AbstractReactiveHealthIndicator.class})
@ConditionalOnBean(LiiklusClient.class)
@ConditionalOnEnabledHealthIndicator("liiklus")
@RequiredArgsConstructor
public class LiiklusReactiveHealthIndicatorAutoConfiguration {

    final LiiklusProperties properties;

    @Bean
    @ConditionalOnMissingBean(name = "liiklusReadHealthIndicator")
    ReactiveHealthIndicator liiklusReadHealthIndicator() {
        URI uri = properties.getReadUri();

        return new AbstractReactiveHealthIndicator() {
            @Override
            protected Mono<Health> doHealthCheck(Health.Builder builder) {
                return Mono
                        .fromSupplier(liiklusHealth(builder, uri))
                        .retry(3)
                        .subscribeOn(Schedulers.immediate())
                        .onErrorResume(__ -> Mono.just(builder.down().build()));
            }
        };
    }

    @Bean
    @ConditionalOnMissingBean(name = "liiklusWriteHealthIndicator")
    @ConditionalOnProperty(prefix = "liiklus", value = "write.uri")
    ReactiveHealthIndicator liiklusWriteHealthIndicator() {
        URI uri = properties.getWriteUri();

        return new AbstractReactiveHealthIndicator() {
            @Override
            protected Mono<Health> doHealthCheck(Health.Builder builder) {
                return Mono
                        .fromSupplier(liiklusHealth(builder, uri))
                        .retry(3)
                        .subscribeOn(Schedulers.immediate())
                        .onErrorResume(__ -> Mono.just(builder.down().build()));
            }
        };
    }

    private Supplier<Health> liiklusHealth(Health.Builder builder, URI uri) {
        return () -> {
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress(uri.getHost(), uri.getPort()), 1000);
                return builder.up().build();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
    }
}
