package io.vivy.liiklus;

import com.github.bsideup.liiklus.container.LiiklusContainer;
import com.github.bsideup.liiklus.protocol.PublishReply;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.internal.verification.AtLeast;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.testcontainers.containers.GenericContainer;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.verify;

@ExtendWith(SpringExtension.class)
@SpringBootTest(
        classes = {AbstractIntegrationTest.WithRecordProcessor.class},
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "liiklus.groupName=${random.uuid}-consumer",
        }
)
public class PropsTest {

    static {
        var liiklus = new LiiklusContainer("0.8.2")
                .withExposedPorts(8081)
                .withEnv("storage_records_type", "MEMORY");

        liiklus.start();

        System.getProperties().putAll(Map.of(
                "liiklus.target", "rsocket://" + liiklus.getContainerIpAddress() + ":" + liiklus.getMappedPort(8081),
                "liiklus.topic", "user-event-log",
                "liiklus.groupVersion", "1",
                "liiklus.ackInterval", "10ms"
        ));
    }

    @Autowired
    protected LiiklusPublisher liiklusPublisher;

    @Test
    void shouldConnectToLiiklusWithTarget() {
        String key = UUID.randomUUID().toString();
        PublishReply offset = liiklusPublisher.publish(key, key.getBytes()).block(Duration.ofSeconds(5));

        assertThat(offset.getTopic()).isNotBlank();
    }
}
