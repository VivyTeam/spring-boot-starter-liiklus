package io.vivy.liiklus;

import com.github.bsideup.liiklus.container.LiiklusContainer;
import com.github.bsideup.liiklus.protocol.PublishReply;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

public class PropsTest {

    static {
        var liiklus = new LiiklusContainer("0.9.0")
                .withExposedPorts(8081);

        liiklus.start();

        System.getProperties().remove("liiklus.read.uri");
        System.getProperties().remove("liiklus.write.uri");

        System.getProperties().putAll(Map.of(
                "liiklus.target", "rsocket://" + liiklus.getContainerIpAddress() + ":" + liiklus.getMappedPort(8081),
                "liiklus.topic", "user-event-log",
                "liiklus.groupVersion", "1",
                "liiklus.ackInterval", "10ms",
                "liiklus.groupName", "consumer-" + UUID.randomUUID()
        ));
    }

    @AfterAll
    static void tearDown() {
        System.getProperties().remove("liiklus.target");
    }

    @Test
    void shouldConnectToLiiklusWithTarget() {
        var ctx = new AnnotationConfigApplicationContext(LiiklusAutoConfiguration.class, LiiklusReactiveHealthIndicatorAutoConfiguration.class);

        var liiklusPublisher = ctx.getBean(LiiklusPublisher.class);

        String key = UUID.randomUUID().toString();
        PublishReply offset = liiklusPublisher.publish(key, key.getBytes()).block(Duration.ofSeconds(5));

        assertThat(offset.getTopic()).isNotBlank();
    }
}
