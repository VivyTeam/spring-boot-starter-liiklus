package io.vivy.liiklus.single;

import com.github.bsideup.liiklus.container.LiiklusContainer;
import com.github.bsideup.liiklus.protocol.PublishReply;
import io.vivy.liiklus.support.LoggingRecordProcessor;
import io.vivy.liiklus.support.TestApplication;
import org.junit.jupiter.api.Test;
import org.mockito.internal.verification.AtLeast;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;

@SpringBootTest(
        classes = {ConnectTest.WithRecordProcessor.class},
        webEnvironment = SpringBootTest.WebEnvironment.MOCK,
        properties = {
                "liiklus.groupName=${random.uuid}-connect",
                "liiklus.write.secret=${random.uuid}",
                "liiklus.topic=user-event-log",
                "liiklus.groupVersion=1",
                "liiklus.ackInterval=10ms",
        }
)
@Testcontainers
public class ConnectTest extends SingleTopicTest {

    @Container
    static LiiklusContainer liiklus = new LiiklusContainer("0.9.3")
            .withExposedPorts(6565, 8081);

    @DynamicPropertySource
    static void liiklusProps(DynamicPropertyRegistry registry) {
        registry.add("liiklus.write.uri", () -> "grpc://" + liiklus.getHost() + ":" + liiklus.getMappedPort(6565));
        registry.add("liiklus.read.uri", () -> "rsocket://" + liiklus.getHost() + ":" + liiklus.getMappedPort(8081));
    }


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
        verify(loggingRecordProcessor, new AtLeast(1)).apply(any(), any());
    }
}
