package services.vivy.liiklus;

import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.given;
import static org.hamcrest.core.Is.is;

class HealthTest extends AbstractIntegrationTest {

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
