package io.vivy.liiklus;

import com.github.bsideup.liiklus.LiiklusClient;
import io.vivy.liiklus.consumer.LiiklusConsumer;
import io.vivy.liiklus.consumer.LiiklusConsumerLoop;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.ReplayProcessor;
import reactor.core.publisher.SignalType;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.logging.Level;

@RequiredArgsConstructor
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
public class LiiklusComponentFactory {

    LiiklusClient liiklusClient;

    Duration ackInterval;

    Scheduler ackScheduler;

    Scheduler readScheduler;

    public LiiklusConsumerLoop createConsumer(String topic, String groupName, int groupVersion, LiiklusConsumer liiklusConsumer) {
        return new LiiklusConsumerLoop(
                topic,
                groupName,
                groupVersion,
                readScheduler,
                liiklusClient,
                (partition, records) -> {
                    var ackInProgress = ReplayProcessor.cacheLastOrDefault(false);
                    var ackFinished = ackInProgress.filter(Boolean.FALSE::equals);

                    return records
                            .concatMap(
                                    record -> Mono.defer(() -> liiklusConsumer.consume(partition, record))
                                            .log("processor", Level.SEVERE, SignalType.ON_ERROR)
                                            .retryWhen(it -> it.delayElements(Duration.ofSeconds(1)))
                                            .delaySubscription(ackFinished)
                                            .thenReturn(record.getOffset()),
                                    1_000
                            )
                            .sample(Flux.interval(ackInterval, ackScheduler))
                            .doOnNext(__ -> ackInProgress.onNext(true))
                            .doOnRequest(__ -> Schedulers.parallel().schedule(() -> ackInProgress.onNext(false)));
                }
        );
    }

}
