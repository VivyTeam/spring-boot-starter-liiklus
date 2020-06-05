package io.vivy.liiklus.single;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.bsideup.liiklus.LiiklusClient;
import com.github.bsideup.liiklus.container.LiiklusContainer;
import com.github.bsideup.liiklus.protocol.PublishReply;
import io.vivy.liiklus.LiiklusPublisher;
import io.vivy.liiklus.support.CompatibleLiiklusPublisher;
import io.vivy.liiklus.support.PartitionAwareLoggingRecordProcessor;
import io.vivy.liiklus.support.TestApplication;
import org.junit.jupiter.api.Test;
import org.mockito.internal.verification.AtLeast;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;

@SpringBootTest(
        classes = {PartitionAwareConnectLiiklusEventTest.WithPartitionAwareRecordProcessor.class},
        webEnvironment = SpringBootTest.WebEnvironment.MOCK,
        properties = {
                "liiklus.groupName=${random.uuid}-partitionaware",
                "liiklus.write.secret=${random.uuid}",
                "liiklus.topic=user-event-log",
                "liiklus.groupVersion=1",
                "liiklus.ackInterval=10ms",
        }
)
@Testcontainers
public class PartitionAwareConnectLiiklusEventTest extends SingleTopicTest {

    @Container
    static LiiklusContainer liiklus = new LiiklusContainer(LiiklusContainer.class.getPackage().getImplementationVersion())
            .withExposedPorts(6565, 8081);

    @DynamicPropertySource
    static void liiklusProps(DynamicPropertyRegistry registry) {
        registry.add("liiklus.write.uri", () -> "grpc://" + liiklus.getHost() + ":" + liiklus.getMappedPort(6565));
        registry.add("liiklus.read.uri", () -> "rsocket://" + liiklus.getHost() + ":" + liiklus.getMappedPort(8081));
    }

    @Autowired
    ObjectMapper objectMapper;

    @SpyBean
    protected PartitionAwareLoggingRecordProcessor partitionAwareLoggingRecordProcessor;

    @Configuration
    @Import(TestApplication.class)
    public static class WithPartitionAwareRecordProcessor {

        @Bean
        public PartitionAwareLoggingRecordProcessor partitionAwareLoggingRecordProcessor() {
            return new PartitionAwareLoggingRecordProcessor();
        }

        @Bean
        @Primary
        public LiiklusPublisher publisher(@Value("${liiklus.topic}") String topic, LiiklusClient liiklusClient) {
            return new CompatibleLiiklusPublisher(topic, liiklusClient);
        }

    }

    @Test
    void shouldReceiveMessages() throws JsonProcessingException {
        String key = UUID.randomUUID().toString();
        var event = Map.of("eventId", UUID.randomUUID().toString(), "content", "constant content");

        PublishReply offset = liiklusPublisher.publish(key, objectMapper.writeValueAsBytes(event)).block(Duration.ofSeconds(5));

        waitForLiiklusOffset(offset);
        verify(partitionAwareLoggingRecordProcessor, new AtLeast(1)).apply(any(), any());
    }

}
