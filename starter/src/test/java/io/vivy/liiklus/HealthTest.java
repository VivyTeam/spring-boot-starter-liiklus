package io.vivy.liiklus;

import io.restassured.builder.RequestSpecBuilder;
import io.restassured.http.ContentType;
import io.restassured.specification.RequestSpecification;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.web.server.LocalServerPort;

import static io.restassured.RestAssured.given;
import static org.hamcrest.core.Is.is;

@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "liiklus.groupName=${random.uuid}-health",
        }
)
class HealthTest extends AbstractIntegrationTest {


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
