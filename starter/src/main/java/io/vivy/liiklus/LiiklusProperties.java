package io.vivy.liiklus;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.Errors;
import org.springframework.validation.Validator;
import org.springframework.validation.annotation.Validated;

import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import java.net.URI;
import java.time.Duration;

@Validated
@ConfigurationProperties("liiklus")
@Data
public class LiiklusProperties {

    URI target;

    @Valid
    Target read;

    @Valid
    Target write;

    @NotEmpty
    String topic;

    @NotNull
    Duration ackInterval = Duration.ofSeconds(5);

    @NotEmpty
    String groupName;

    @Min(1)
    int groupVersion = 1;

    public URI getTargetURI() {
        return target;
    }

    public URI getReadUri() {
        return read == null ? target : read.getUri();
    }

    public URI getWriteUri() {
        return write == null ? target : write.getUri();
    }

    @Data
    public static class Target {
        @NotNull
        URI uri;
    }


    public static class LiiklusPropertiesValidator implements Validator {
        @Override
        public boolean supports(Class<?> type) {
            return type == LiiklusProperties.class;
        }

        @Override
        public void validate(Object o, Errors errors) {
            var properties = (LiiklusProperties) o;

            if (properties.getTarget() == null && (properties.getRead() == null || properties.getWrite() == null)) {
                errors.reject("target", "target or read.uri and write.uri should be non-empty URI");
            }
        }
    }

}
