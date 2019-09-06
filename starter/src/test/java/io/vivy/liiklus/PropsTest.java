package io.vivy.liiklus;

import com.github.bsideup.liiklus.container.LiiklusContainer;
import com.github.bsideup.liiklus.protocol.PublishReply;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.mock.env.MockEnvironment;

import java.time.Duration;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PropsTest {

    static LiiklusContainer liiklus;

    static {
        liiklus = new LiiklusContainer("0.9.0")
                .withExposedPorts(8081);

        liiklus.start();
    }

    @Test
    void shouldConnectToLiiklusWithTarget() {
        var ctx = new AnnotationConfigApplicationContext();
        ctx.register(LiiklusAutoConfiguration.class, LiiklusReactiveHealthIndicatorAutoConfiguration.class);
        ctx.setEnvironment(new MockEnvironment()
                .withProperty("liiklus.target", "rsocket://" + liiklus.getContainerIpAddress() + ":" + liiklus.getMappedPort(8081))
                .withProperty("liiklus.topic", "user-event-log")
                .withProperty("liiklus.groupVersion", "1")
                .withProperty("liiklus.ackInterval", "10ms")
                .withProperty("liiklus.groupName", "consumer-" + UUID.randomUUID())
        );
        ctx.refresh();

        var liiklusPublisher = ctx.getBean(LiiklusPublisher.class);

        String key = UUID.randomUUID().toString();
        PublishReply offset = liiklusPublisher.publish(key, key.getBytes()).block(Duration.ofSeconds(5));

        assertThat(offset.getTopic()).isNotBlank();
    }

    @Test
    void shouldFailToWriteOnEmptyWrite() {
        var ctx = new AnnotationConfigApplicationContext();
        ctx.register(LiiklusAutoConfiguration.class, LiiklusReactiveHealthIndicatorAutoConfiguration.class);
        ctx.setEnvironment(new MockEnvironment()
                .withProperty("liiklus.read.uri", "rsocket://" + liiklus.getContainerIpAddress() + ":" + liiklus.getMappedPort(8081))
                .withProperty("liiklus.topic", "user-event-log")
                .withProperty("liiklus.groupVersion", "1")
                .withProperty("liiklus.ackInterval", "10ms")
                .withProperty("liiklus.groupName", "consumer-" + UUID.randomUUID())
        );
        ctx.refresh();

        var liiklusPublisher = ctx.getBean(LiiklusPublisher.class);

        String key = UUID.randomUUID().toString();
        assertThatThrownBy(() -> liiklusPublisher.publish(key, key.getBytes()).block(Duration.ofSeconds(5)))
                .isInstanceOf(IllegalCallerException.class);
    }

    @Test
    void shouldWorkOnOnlyWrite() {
        var ctx = new AnnotationConfigApplicationContext();
        ctx.register(LiiklusAutoConfiguration.class, LiiklusReactiveHealthIndicatorAutoConfiguration.class);
        ctx.setEnvironment(new MockEnvironment()
                .withProperty("liiklus.write.uri", "rsocket://" + liiklus.getContainerIpAddress() + ":" + liiklus.getMappedPort(8081))
                .withProperty("liiklus.topic", "user-event-log")
                .withProperty("liiklus.groupVersion", "1")
                .withProperty("liiklus.ackInterval", "10ms")
                .withProperty("liiklus.groupName", "consumer-" + UUID.randomUUID())
        );
        ctx.refresh();

        var liiklusPublisher = ctx.getBean(LiiklusPublisher.class);
        String key = UUID.randomUUID().toString();
        PublishReply offset = liiklusPublisher.publish(key, key.getBytes()).block(Duration.ofSeconds(5));

        assertThat(offset.getTopic()).isNotBlank();
    }
}
