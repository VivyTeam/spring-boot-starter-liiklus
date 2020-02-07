package io.vivy.liiklus.single;

import com.github.bsideup.liiklus.protocol.ReceiveReply;
import io.vivy.liiklus.common.Liiklus;
import io.vivy.liiklus.consumer.LiiklusConsumer;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;

@Slf4j
@Liiklus(prefix = "test")
public class TestConsumer extends LiiklusConsumer {

    @Override
    public Mono<Void> consume(int partition, ReceiveReply.Record record) {
        log.info("{} [{}:{}] [{}] {}", record.getTimestamp(), partition, record.getOffset(), record.getKey(), record.getValue().toStringUtf8());
        return Mono.empty();
    }
}
