package io.vivy.liiklus.multiple;

import com.github.bsideup.liiklus.container.LiiklusContainer;
import io.vivy.liiklus.LiiklusAutoConfiguration;
import org.junit.jupiter.api.Test;
import org.mockito.internal.verification.Times;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.testcontainers.lifecycle.Startables;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.verify;

@SpringBootTest(
        classes = {LiiklusAutoConfiguration.class, UserTopicConfiguration.class, GlobalTopicConfiguration.class},
        webEnvironment = SpringBootTest.WebEnvironment.MOCK,
        properties = {
                "user.topic=user-event-log",
                "user.groupName=service1",
                "user.groupVersion=1",
                "global.topic=global-event-log",
                "global.groupName=service2",
                "global.groupVersion=3",
        }
)
public class MultipleTopicTest {

    static {
        var liiklus = new LiiklusContainer("0.9.0")
                .withExposedPorts(6565, 8081);

        Startables.deepStart(List.of(liiklus)).join();

        System.getProperties().putAll(Map.of(
                "liiklus.write.uri", "grpc://" + liiklus.getContainerIpAddress() + ":" + liiklus.getMappedPort(6565),
                "liiklus.write.secret", UUID.randomUUID().toString(),
                "liiklus.read.uri", "rsocket://" + liiklus.getContainerIpAddress() + ":" + liiklus.getMappedPort(8081),
                "liiklus.ackInterval", "10ms"
        ));
    }

    @SpyBean
    protected UserTopicConfiguration.UserConsumer userConsumer;

    @SpyBean
    protected GlobalTopicConfiguration.GlobalConsumer globalConsumer;

    @Autowired
    protected UserTopicConfiguration.UserPublisher userPublisher;

    @Autowired
    protected GlobalTopicConfiguration.GlobalPublisher globalPublisher;

    @Test
    void shouldReceiveMessages() {
        String key = UUID.randomUUID().toString();
        globalPublisher.publish(key, key.getBytes()).block(Duration.ofSeconds(5));
        userPublisher.publish(key, key.getBytes()).block(Duration.ofSeconds(5));

        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            verify(userConsumer, new Times(1)).consume(anyInt(), any());
        });
        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            verify(globalConsumer, new Times(1)).consume(anyInt(), any());
        });
    }
}
