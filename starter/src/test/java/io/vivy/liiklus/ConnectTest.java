package io.vivy.liiklus;

import com.github.bsideup.liiklus.protocol.PublishReply;
import io.vivy.liiklus.support.LoggingRecordProcessor;
import org.junit.jupiter.api.Test;
import org.mockito.internal.verification.AtLeast;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import java.time.Duration;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.verify;

@SpringBootTest(
        classes = {ConnectTest.WithRecordProcessor.class},
        webEnvironment = SpringBootTest.WebEnvironment.MOCK,
        properties = {
                "liiklus.groupName=${random.uuid}-connect",
        }
)
public class ConnectTest extends AbstractIntegrationTest {

    @SpyBean
    protected LoggingRecordProcessor loggingRecordProcessor;

    @Configuration
    @Import(TestApplication.class)
    public static class WithRecordProcessor {

        @Bean
        public LoggingRecordProcessor loggingRecordProcessor() {
            return new LoggingRecordProcessor();
        }

    }

    @Test
    void shouldReceiveMessages() {
        String key = UUID.randomUUID().toString();
        PublishReply offset = liiklusPublisher.publish(key, key.getBytes()).block(Duration.ofSeconds(5));

        waitForLiiklusOffset(offset);
        verify(loggingRecordProcessor, new AtLeast(1)).apply(anyInt(), any());
    }
}
