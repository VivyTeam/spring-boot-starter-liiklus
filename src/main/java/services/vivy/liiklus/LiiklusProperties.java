package services.vivy.liiklus;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import java.net.URI;
import java.time.Duration;

@Validated
@ConfigurationProperties("liiklus")
@Data
public class LiiklusProperties {

    @NotEmpty
    String target;

    @NotEmpty
    String topic;

    @NotNull
    Duration ackInterval = Duration.ofSeconds(5);

    @NotEmpty
    String groupName;

    @Min(1)
    int groupVersion = 1;

    public URI getTargetURI() {
        return URI.create(target);
    }
}
