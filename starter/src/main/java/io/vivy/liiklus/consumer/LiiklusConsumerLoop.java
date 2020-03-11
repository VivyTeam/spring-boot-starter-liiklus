package io.vivy.liiklus.consumer;

import com.github.bsideup.liiklus.LiiklusClient;
import com.github.bsideup.liiklus.protocol.AckRequest;
import com.github.bsideup.liiklus.protocol.Assignment;
import com.github.bsideup.liiklus.protocol.ReceiveReply;
import com.github.bsideup.liiklus.protocol.ReceiveRequest;
import com.github.bsideup.liiklus.protocol.SubscribeReply;
import com.github.bsideup.liiklus.protocol.SubscribeRequest;
import com.google.protobuf.Empty;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.experimental.NonFinal;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Publisher;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.core.scheduler.Scheduler;

import java.time.Duration;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.logging.Level;

@Slf4j
@RequiredArgsConstructor
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
@Getter
public class LiiklusConsumerLoop implements AutoCloseable {

    String topic;
    String groupName;
    int groupVersion;

    Scheduler readScheduler;
    Scheduler ackScheduler;

    LiiklusClient client;
    BiFunction<Integer, Flux<ReceiveReply.Record>, Publisher<Long>> liiklusRecordProcessor;

    @NonFinal
    Disposable disposable;

    public void run() {
        if (Objects.nonNull(disposable)) {
            return;
        }
        SubscribeRequest subscribeAction = SubscribeRequest.newBuilder()
                .setTopic(topic)
                .setGroup(groupName)
                .setGroupVersion(groupVersion)
                .setAutoOffsetReset(SubscribeRequest.AutoOffsetReset.EARLIEST)
                .build();

        disposable = Flux.defer(() -> client.subscribe(subscribeAction))
                .filter(SubscribeReply::hasAssignment)
                .map(SubscribeReply::getAssignment)
                .onBackpressureBuffer()
                .groupBy(Assignment::getPartition, Integer.MAX_VALUE)
                .flatMap(partitionAssignments -> {
                    int partition = partitionAssignments.key();

                    return partitionAssignments
                            .switchMap(assignment -> client.receive(ReceiveRequest.newBuilder().setAssignment(assignment).build()))
                            .filter(ReceiveReply::hasRecord)
                            .map(ReceiveReply::getRecord)
                            .compose(it -> liiklusRecordProcessor.apply(partition, it))
                            .flatMap(offset -> sendAck(partition, offset), 1, 1);
                }, Integer.MAX_VALUE, Integer.MAX_VALUE)
                .log("mainLoop", Level.WARNING, SignalType.ON_ERROR)
                .retryWhen(it -> it.delayElements(Duration.ofSeconds(1)))
                .subscribeOn(readScheduler)
                .subscribe();
    }

    private Mono<Empty> sendAck(int partition, long offset) {
        AckRequest request = AckRequest.newBuilder()
                .setTopic(topic)
                .setGroup(groupName)
                .setGroupVersion(groupVersion)
                .setPartition(partition)
                .setOffset(offset)
                .build();
        return Mono
                .defer(() -> client.ack(request))
                .log("p" + partition + "-ack", Level.WARNING, SignalType.ON_ERROR)
                .retryWhen(it -> it.delayElements(Duration.ofSeconds(1)));
    }

    @Override
    public void close() {
        if (disposable != null) {
            disposable.dispose();
        }
        if (readScheduler != null) {
            readScheduler.dispose();
        }
        if (ackScheduler != null) {
            ackScheduler.dispose();
        }
    }
}
