package io.vivy.liiklus;

import com.github.bsideup.liiklus.protocol.PublishReply;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;

public class ConnectTest extends AbstractIntegrationTest {

    @Test
    void shouldReceiveMessages() {
        String key = UUID.randomUUID().toString();
        PublishReply offset = liiklusPublisher.publish(key, key.getBytes()).block(Duration.ofSeconds(5));

        waitForLiiklusOffset(offset);
        verify(loggingRecordProcessor).apply(any());
    }
}
