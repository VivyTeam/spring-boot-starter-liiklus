package io.vivy.liiklus.single;

import com.github.bsideup.liiklus.protocol.PublishReply;
import io.vivy.liiklus.LiiklusAutoConfiguration;
import org.junit.jupiter.api.Test;
import org.mockito.internal.verification.AtLeast;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;

import java.time.Duration;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.verify;

@SpringBootTest(
        classes = {TestConfiguration.class, LiiklusAutoConfiguration.class},
        webEnvironment = SpringBootTest.WebEnvironment.MOCK,
        properties = {
                "test.groupName=${random.uuid}-connect",
        }
)
public class ConnectTest extends SingleTopicTest {

    @SpyBean
    protected TestConsumer testConsumer;

    @Autowired
    protected TestPublisher testPublisher;

    @Test
    void shouldReceiveMessages() {
        String key = UUID.randomUUID().toString();
        PublishReply offset = testPublisher.publish(key, key.getBytes()).block(Duration.ofSeconds(5));

        waitForLiiklusOffset(offset);
        verify(testConsumer, new AtLeast(1)).consume(anyInt(), any());
    }
}
