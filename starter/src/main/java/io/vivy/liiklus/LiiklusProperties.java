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
import javax.validation.constraints.NotNull;
import java.net.URI;
import java.time.Duration;
import java.util.Map;

@Validated
@ConfigurationProperties("liiklus")
@Data
public class LiiklusProperties {

    URI target;

    @Valid
    Target read;

    @Valid
    Target write;

    @NotNull
    Duration ackInterval = Duration.ofSeconds(5);

    @Deprecated
    String topic;

    @Deprecated
    String groupName;

    @Min(1)
    @Deprecated
    int groupVersion = 1;

    Map<String, String> topics;

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Target {
        @NotNull
        URI uri;

        String secret;
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
            if (properties.getTopic() != null && properties.getTopic().isBlank()) {
                errors.reject("topic", "topic can't be blank");
            }
            if (properties.getGroupName() != null && properties.getGroupName().isBlank()) {
                errors.reject("groupName", "groupName can't be blank");
            }
        }
    }

}
