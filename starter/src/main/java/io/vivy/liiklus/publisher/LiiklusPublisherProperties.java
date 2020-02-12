package io.vivy.liiklus.publisher;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.springframework.core.env.Environment;

@RequiredArgsConstructor
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
@Getter
public class LiiklusPublisherProperties {

    String topic;

    public static LiiklusPublisherProperties create(Environment environment, String prefix) {
        var topic = environment.getRequiredProperty(prefix + ".topic");
        if (topic.isBlank()) {
            throw new IllegalStateException(prefix + ".topic can not be blank");
        }
        return new LiiklusPublisherProperties(topic);
    }
}
