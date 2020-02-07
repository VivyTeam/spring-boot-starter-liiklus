package io.vivy.liiklus.single;

import com.github.bsideup.liiklus.container.LiiklusContainer;
import com.github.bsideup.liiklus.protocol.PublishReply;
import io.vivy.liiklus.LiiklusAutoConfiguration;
import io.vivy.liiklus.publisher.LiiklusPublisher;
import io.vivy.liiklus.LiiklusReactiveHealthIndicatorAutoConfiguration;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
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
        ctx.register(TestConfiguration.class, LiiklusAutoConfiguration.class, LiiklusReactiveHealthIndicatorAutoConfiguration.class);
        ctx.setEnvironment(new MockEnvironment()
                .withProperty("liiklus.target", "rsocket://" + liiklus.getContainerIpAddress() + ":" + liiklus.getMappedPort(8081))
                .withProperty("liiklus.ackInterval", "10ms")
                .withProperty("test.topic", "user-event-log")
                .withProperty("test.groupVersion", "1")
                .withProperty("test.groupName", "consumer-" + UUID.randomUUID())
        );
        ctx.refresh();

        var testPublisher = ctx.getBean(TestPublisher.class);

        String key = UUID.randomUUID().toString();
        PublishReply offset = testPublisher.publish(key, key.getBytes()).block(Duration.ofSeconds(5));

        assertThat(offset.getTopic()).isNotBlank();
    }

    @Test
    void shouldFailToWriteOnEmptyWrite() {
        var ctx = new AnnotationConfigApplicationContext();
        ctx.register(TestConfiguration.class, LiiklusAutoConfiguration.class, LiiklusReactiveHealthIndicatorAutoConfiguration.class);
        ctx.setEnvironment(new MockEnvironment()
                .withProperty("liiklus.read.uri", "rsocket://" + liiklus.getContainerIpAddress() + ":" + liiklus.getMappedPort(8081))
                .withProperty("liiklus.ackInterval", "10ms")
                .withProperty("test.topic", "user-event-log")
                .withProperty("test.groupVersion", "1")
                .withProperty("test.groupName", "consumer-" + UUID.randomUUID())
        );
        ctx.refresh();

        var testPublisher = ctx.getBean(TestPublisher.class);

        String key = UUID.randomUUID().toString();
        assertThatThrownBy(() -> testPublisher.publish(key, key.getBytes()).block(Duration.ofSeconds(5)))
                .isInstanceOf(IllegalCallerException.class);
    }

    @Test
    void shouldWorkOnOnlyWrite() {
        var ctx = new AnnotationConfigApplicationContext();
        ctx.register(TestConfiguration.class, LiiklusAutoConfiguration.class, LiiklusReactiveHealthIndicatorAutoConfiguration.class);
        ctx.setEnvironment(new MockEnvironment()
                .withProperty("liiklus.write.uri", "rsocket://" + liiklus.getContainerIpAddress() + ":" + liiklus.getMappedPort(8081))
                .withProperty("liiklus.ackInterval", "10ms")
                .withProperty("test.topic", "user-event-log")
                .withProperty("test.groupVersion", "1")
                .withProperty("test.groupName", "consumer-" + UUID.randomUUID())
        );
        ctx.refresh();

        var testPublisher = ctx.getBean(TestPublisher.class);
        String key = UUID.randomUUID().toString();
        PublishReply offset = testPublisher.publish(key, key.getBytes()).block(Duration.ofSeconds(5));

        assertThat(offset.getTopic()).isNotBlank();
    }
}
