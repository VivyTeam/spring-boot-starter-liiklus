package io.vivy.liiklus;

import com.github.bsideup.liiklus.protocol.ReceiveReply;
import com.google.protobuf.util.Timestamps;
import io.vivy.liiklus.event.EventLogProcessor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Instant;

@Service
@Slf4j
public class LoggingRecordProcessor implements RecordProcessor {

    @Override
    public Mono<Void> apply(ReceiveReply.Record record) {
        return new EventLogProcessor<ReceiveReply.Record>() {
            @Override
            public Mono<Void> apply(Event<ReceiveReply.Record> recordEvent) {
                log.info("Received record: {}:{}", recordEvent.getOffset(), recordEvent.getValue());
                return Mono.empty();
            }
        }.apply(new EventLogProcessor.Event<>() {
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
        });
    }
}
