package io.vivy.liiklus;

import com.github.bsideup.liiklus.protocol.PublishReply;
import com.github.bsideup.liiklus.protocol.ReceiveReply;
import com.google.protobuf.util.Timestamps;
import io.vivy.liiklus.event.EventLogProcessor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.internal.verification.AtLeast;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;

@ExtendWith(SpringExtension.class)
@SpringBootTest(
        classes = {PartitionAwareConnectTest.WithPartitionAwareRecordProcessor.class},
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "liiklus.groupName=${random.uuid}-partitionaware",
        }
)
public class PartitionAwareConnectTest extends AbstractIntegrationTest {

    @SpyBean
    protected PartitionAwareLoggingRecordProcessor partitionAwareLoggingRecordProcessor;

    @Test
    void shouldReceiveMessages() {
        String key = UUID.randomUUID().toString();
        PublishReply offset = liiklusPublisher.publish(key, key.getBytes()).block(Duration.ofSeconds(5));

        waitForLiiklusOffset(offset);
        verify(partitionAwareLoggingRecordProcessor, new AtLeast(1)).apply(any(), any());
    }

    @Configuration
    @Import(TestApplication.class)
    public static class WithPartitionAwareRecordProcessor {

        @Bean
        public PartitionAwareLoggingRecordProcessor partitionAwareLoggingRecordProcessor() {
            return new PartitionAwareLoggingRecordProcessor();
        }

    }

    @Slf4j
    public static class PartitionAwareLoggingRecordProcessor implements PartitionAwareProcessor {

        @Override
        public Mono<Void> apply(Integer passedPartition, ReceiveReply.Record record) {
            return ((EventLogProcessor<ReceiveReply.Record>) recordEvent -> {
                log.info("Received record: {}:{}", recordEvent.getOffset(), recordEvent.getValue());
                return Mono.empty();
            })
                    .apply(new EventLogProcessor.Event<>() {

                        @Getter(lazy = true)
                        private final int partition = passedPartition;

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
}
