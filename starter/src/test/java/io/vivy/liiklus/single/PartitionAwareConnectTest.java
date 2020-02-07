package io.vivy.liiklus.single;

import com.github.bsideup.liiklus.protocol.PublishReply;
import io.vivy.liiklus.LiiklusAutoConfiguration;
import io.vivy.liiklus.consumer.LiiklusConsumerLoop;
import io.vivy.liiklus.publisher.LiiklusPublisher;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.internal.verification.AtLeast;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.time.Duration;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.verify;

@ExtendWith(SpringExtension.class)
@SpringBootTest(
        classes = {TestConfiguration.class, LiiklusAutoConfiguration.class},
        webEnvironment = SpringBootTest.WebEnvironment.MOCK,
        properties = {
                "test.groupName=p${random.uuid}-artitionaware",
        }
)
public class PartitionAwareConnectTest extends AbstractIntegrationTest {

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
