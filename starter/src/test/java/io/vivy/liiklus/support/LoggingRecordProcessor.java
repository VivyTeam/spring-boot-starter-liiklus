package io.vivy.liiklus.support;

import com.github.bsideup.liiklus.protocol.ReceiveReply;
import com.google.protobuf.util.Timestamps;
import io.vivy.liiklus.PartitionAwareProcessor;
import io.vivy.liiklus.event.EventLogProcessor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;

import java.time.Instant;

@Slf4j
public class LoggingRecordProcessor implements PartitionAwareProcessor {

    @Override
    public Mono<Void> apply(Integer part, ReceiveReply.Record record) {
        return ((EventLogProcessor<ReceiveReply.Record>) recordEvent -> {
            log.info("{} [{}:{}] [{}] {}", recordEvent.getTimestamp(), recordEvent.getPartition(), recordEvent.getOffset(), recordEvent.getKey(), record.getValue().toStringUtf8());
            return Mono.empty();
        })
                .apply(new EventLogProcessor.Event<>() {

                    @Getter
                    private final int partition = part;

                    @Getter
                    private final long offset = record.getOffset();

                    @Getter(lazy = true)
                    private final String key = record.getKey().toStringUtf8();

                    @Getter
                    private final ReceiveReply.Record value = record;

                    @Getter(lazy = true)
                    private final Instant timestamp = Instant.ofEpochMilli(Timestamps.toMillis(record.getTimestamp()));

                    @Getter
                    public final boolean replay = record.getReplay();

                })
                .then();
    }

}