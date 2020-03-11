package io.vivy.liiklus.producer;

import io.vivy.liiklus.LiiklusProperties;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;

import static java.util.Objects.isNull;

@RequiredArgsConstructor
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
@Getter
public class LiiklusProducerProperties {

    String topic;

    public static LiiklusProducerProperties create(LiiklusProperties properties, String name) {
        var topic = properties.getTopics().get(name + ".topic");
        if (isNull(topic) ||  topic.isBlank()) {
            throw new IllegalStateException(name + ".topic can not be null or blank");
        }
        return new LiiklusProducerProperties(topic);
    }
}
