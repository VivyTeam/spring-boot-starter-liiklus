package io.vivy.liiklus.consumer;

import com.github.bsideup.liiklus.protocol.ReceiveReply;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import reactor.core.publisher.Mono;

import java.util.Objects;

@RequiredArgsConstructor
@FieldDefaults(makeFinal = false)
@Getter
public abstract class LiiklusConsumer implements AutoCloseable {

    protected LiiklusConsumerLoop consumerLoop;

    public void init(LiiklusConsumerLoop consumerLoop) {
        this.consumerLoop = consumerLoop;
    }

    public abstract Mono<Void> consume(int partition, ReceiveReply.Record record);

    @Override
    public void close() {
        if (Objects.isNull(consumerLoop)) {
            return;
        }
        consumerLoop.close();
    }

}
