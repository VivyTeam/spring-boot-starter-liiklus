package io.vivy.liiklus.consumer;

import com.github.bsideup.liiklus.LiiklusClient;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.ReplayProcessor;
import reactor.core.publisher.SignalType;
import reactor.core.scheduler.Schedulers;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.logging.Level;

@RequiredArgsConstructor
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
public class LiiklusConsumerFactory {

    LiiklusClient liiklusClient;

    Duration ackInterval;

    public LiiklusConsumerLoop createConsumer(String topic, String groupName, int groupVersion, LiiklusConsumer liiklusConsumer) {
        var ackScheduler = Schedulers.newSingle(String.format("ack_%s", topic));
        var readScheduler = Schedulers.newParallel(String.format("liiklus_%s", topic));
        return new LiiklusConsumerLoop(
                topic,
                groupName,
                groupVersion,
                readScheduler,
                ackScheduler,
                liiklusClient,
                (partition, records) -> {
                    var ackInProgress = ReplayProcessor.cacheLastOrDefault(false);
                    var ackFinished = ackInProgress.filter(Boolean.FALSE::equals);

                    return records
                            .concatMap(
                                    record -> Mono.defer(() -> liiklusConsumer.consume(partition, record))
                                            .log("processor", Level.SEVERE, SignalType.ON_ERROR)
                                            .retryWhen(Retry.fixedDelay(Long.MAX_VALUE, Duration.ofSeconds(1)))
                                            .delaySubscription(ackFinished)
                                            .thenReturn(record.getOffset()),
                                    1_000
                            )
                            .sample(Flux.interval(ackInterval, ackScheduler))
                            .onBackpressureLatest()
                            .doOnNext(__ -> ackInProgress.onNext(true))
                            .doOnRequest(__ -> Schedulers.parallel().schedule(() -> ackInProgress.onNext(false)));
                }
        );
    }

}
