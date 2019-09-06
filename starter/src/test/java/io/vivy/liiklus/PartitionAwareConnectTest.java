package io.vivy.liiklus;

import com.github.bsideup.liiklus.protocol.PublishReply;
import io.vivy.liiklus.support.PartitionAwareLoggingRecordProcessor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.internal.verification.AtLeast;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.time.Duration;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;

@ExtendWith(SpringExtension.class)
@SpringBootTest(
        classes = {PartitionAwareConnectTest.WithPartitionAwareRecordProcessor.class},
        webEnvironment = SpringBootTest.WebEnvironment.MOCK,
        properties = {
                "liiklus.groupName=${random.uuid}-partitionaware",
        }
)
public class PartitionAwareConnectTest extends AbstractIntegrationTest {

    @SpyBean
    protected PartitionAwareLoggingRecordProcessor partitionAwareLoggingRecordProcessor;

    @Configuration
    @Import(TestApplication.class)
    public static class WithPartitionAwareRecordProcessor {

        @Bean
        public PartitionAwareLoggingRecordProcessor partitionAwareLoggingRecordProcessor() {
            return new PartitionAwareLoggingRecordProcessor();
        }

    }

    @Test
    void shouldReceiveMessages() {
        String key = UUID.randomUUID().toString();
        PublishReply offset = liiklusPublisher.publish(key, key.getBytes()).block(Duration.ofSeconds(5));

        waitForLiiklusOffset(offset);
        verify(partitionAwareLoggingRecordProcessor, new AtLeast(1)).apply(any(), any());
    }

}
