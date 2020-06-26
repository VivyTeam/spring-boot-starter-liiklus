package io.vivy.liiklus.consumer;

import com.github.bsideup.liiklus.protocol.ReceiveReply;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.time.Duration;

class ConsumerTest {

    @Test
    @Timeout(5)
    void shouldPassOnlySamplesToAck() {
        StepVerifier.withVirtualTime(
                () -> Flux.interval(Duration.ofSeconds(1))
                        .map(i -> ReceiveReply.Record.newBuilder().setOffset(i).build())
                        .log("records")
                        .transformDeferred(
                                records -> new LiiklusConsumerFactory.PartitionHandler(
                                        new LiiklusConsumer() {
                                            @Override
                                            public Mono<Void> consume(int partition, ReceiveReply.Record record) {
                                                return Mono.empty();
                                            }
                                        },
                                        Schedulers.parallel(),
                                        Duration.ofSeconds(5)
                                ).apply(0, records)
                        )
                        .log("flow"),
                1
        )
                .expectSubscription()
                .thenAwait(Duration.ofSeconds(10))
                .expectNextCount(1)
                .expectNoEvent(Duration.ofMinutes(1))
                .thenRequest(1)
                .thenAwait(Duration.ofSeconds(5))
                .expectNextCount(1)
                .thenCancel()
                .verify();
    }

    @Test
    @Timeout(5)
    void shouldProduceLackRequestsError() {
        StepVerifier.withVirtualTime(() -> Flux
                        .interval(Duration.ofSeconds(1)).log("interval")
                        .sample(Duration.ofSeconds(5)).log("sample")
                , 1)
                .expectSubscription()
                .thenAwait(Duration.ofSeconds(10))
                .expectNextCount(1)
                .expectError()
                .verify();
    }

    @Test
    @Timeout(5)
    void shouldNotProduceLackRequestsError() {
        StepVerifier.withVirtualTime(() -> Flux
                        .interval(Duration.ofSeconds(1)).log("interval")
                        .sample(Duration.ofSeconds(5)).log("sample")
                        .onBackpressureLatest().log("backpressure")
                , 1)
                .expectSubscription()
                .thenAwait(Duration.ofSeconds(10))
                .expectNext(3L)
                .thenRequest(1)
                .thenAwait(Duration.ofSeconds(10))
                .expectNext(8L)
                .thenCancel()
                .verify();
    }
}
