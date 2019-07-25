package io.vivy.liiklus;

import com.github.bsideup.liiklus.LiiklusClient;
import com.github.bsideup.liiklus.container.LiiklusContainer;
import com.github.bsideup.liiklus.protocol.GetOffsetsRequest;
import com.github.bsideup.liiklus.protocol.PublishReply;
import io.restassured.builder.RequestSpecBuilder;
import io.restassured.http.ContentType;
import io.restassured.specification.RequestSpecification;
import org.assertj.core.api.Condition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.testcontainers.containers.GenericContainer;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@ExtendWith(SpringExtension.class)
@SpringBootTest(
        classes = {TestApplication.class},
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT
)

public class AbstractIntegrationTest {

    static {
        LiiklusContainer liiklus = new LiiklusContainer("0.8.2")
                .withExposedPorts(8081)
                .withEnv("storage_records_type", "MEMORY");

        Stream.of(liiklus).parallel().forEach(GenericContainer::start);

        System.getProperties().putAll(Map.of(
                "liiklus.target", "rsocket://" + liiklus.getContainerIpAddress() + ":" + liiklus.getMappedPort(8081),
                "liiklus.topic", "user-event-log",
                "liiklus.groupName", "consumer",
                "liiklus.groupVersion", "1",
                "liiklus.ackInterval", "10ms"
        ));
    }

    @Autowired
    protected LiiklusPublisher liiklusPublisher;

    @Autowired
    protected LiiklusClient liiklusClient;

    @Autowired
    protected LiiklusProperties liiklusProperties;

    @SpyBean
    protected LoggingRecordProcessor loggingRecordProcessor;

    @LocalServerPort
    private int port;

    protected RequestSpecification requestSpecification;

    @BeforeEach
    void setUpRequestSpecification() {
        requestSpecification = new RequestSpecBuilder()
                .setContentType(ContentType.JSON)
                .setPort(port)
                .build();
    }

    protected void waitForLiiklusOffset(PublishReply latestOffset) {
        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            GetOffsetsRequest getOffsetsRequest = GetOffsetsRequest.newBuilder()
                    .setTopic(liiklusProperties.getTopic())
                    .setGroup(liiklusProperties.getGroupName())
                    .setGroupVersion(liiklusProperties.getGroupVersion())
                    .build();
            Condition<Long> offsetCondition = new Condition<>(
                    it -> it >= latestOffset.getOffset(),
                    "Offset is >= then " + latestOffset.getOffset()
            );

            assertThat(liiklusClient.getOffsets(getOffsetsRequest).block(Duration.ofSeconds(5)).getOffsetsMap())
                    .hasEntrySatisfying(latestOffset.getPartition(), offsetCondition);
        });

    }
}
