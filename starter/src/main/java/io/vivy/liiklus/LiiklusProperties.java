package io.vivy.liiklus;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
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

    /**
     * @deprecated use {@link #getTarget()} directly
     */
    @Deprecated
    public URI getTargetURI() {
        return getTarget();
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
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

            if (properties.getTarget() == null && properties.getRead() == null && properties.getWrite() == null) {
                errors.reject("target", "at least one of the target, read.uri or write.uri should be non-empty URI");
            }
        }
    }

}
