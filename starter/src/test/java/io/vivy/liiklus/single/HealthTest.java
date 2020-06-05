package io.vivy.liiklus.single;

import com.github.bsideup.liiklus.container.LiiklusContainer;
import io.restassured.builder.RequestSpecBuilder;
import io.restassured.http.ContentType;
import io.restassured.specification.RequestSpecification;
import io.vivy.liiklus.support.TestApplication;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static io.restassured.RestAssured.given;
import static org.hamcrest.core.Is.is;

@SpringBootTest(
        classes = TestApplication.class,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "liiklus.groupName=${random.uuid}-health",
                "liiklus.groupName=${random.uuid}-partitionaware",
                "liiklus.write.secret=${random.uuid}",
                "liiklus.topic=user-event-log",
                "liiklus.groupVersion=1",
                "liiklus.ackInterval=10ms",
        }
)
@Testcontainers
class HealthTest extends SingleTopicTest {

    @Container
    static LiiklusContainer liiklus = new LiiklusContainer(LiiklusContainer.class.getPackage().getImplementationVersion())
            .withExposedPorts(6565, 8081);

    @DynamicPropertySource
    static void liiklusProps(DynamicPropertyRegistry registry) {
        registry.add("liiklus.write.uri", () -> "grpc://" + liiklus.getHost() + ":" + liiklus.getMappedPort(6565));
        registry.add("liiklus.read.uri", () -> "rsocket://" + liiklus.getHost() + ":" + liiklus.getMappedPort(8081));
    }

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

    @Test
    void shouldBeHealthy() {
        given(requestSpecification)
                .when()
                .get("/health")
                .then()
                .statusCode(200)
                .body("status", is("UP"));
    }
}
