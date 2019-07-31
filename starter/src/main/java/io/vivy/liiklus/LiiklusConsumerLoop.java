package io.vivy.liiklus;

import com.github.bsideup.liiklus.LiiklusClient;
import com.github.bsideup.liiklus.protocol.AckRequest;
import com.github.bsideup.liiklus.protocol.Assignment;
import com.github.bsideup.liiklus.protocol.ReceiveReply;
import com.github.bsideup.liiklus.protocol.ReceiveRequest;
import com.github.bsideup.liiklus.protocol.SubscribeReply;
import com.github.bsideup.liiklus.protocol.SubscribeRequest;
import com.google.protobuf.Empty;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Publisher;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.logging.Level;

@RequiredArgsConstructor
@Slf4j
public class LiiklusConsumerLoop implements ApplicationRunner, AutoCloseable {

    final LiiklusClient client;

    final LiiklusProperties properties;

    final BiFunction<Integer, Flux<ReceiveReply.Record>, Publisher<Long>> liiklusRecordProcessor;

    Disposable disposable;

    @Override
    public void run(ApplicationArguments args) {
        SubscribeRequest subscribeAction = SubscribeRequest.newBuilder()
                .setTopic(properties.getTopic())
                .setGroup(properties.getGroupName())
                .setGroupVersion(properties.getGroupVersion())
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
                .subscribeOn(Schedulers.newParallel("liiklus"))
                .subscribe();
    }

    private Mono<Empty> sendAck(int partition, long offset) {
        AckRequest request = AckRequest.newBuilder()
                .setTopic(properties.getTopic())
                .setGroup(properties.getGroupName())
                .setGroupVersion(properties.getGroupVersion())
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
    }
}
