package io.vivy.liiklus.consumer;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.springframework.core.env.Environment;

@RequiredArgsConstructor
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
@Getter
public class LiiklusConsumerProperties {

    String topic;
    String groupName;
    int groupVersion;

    public static LiiklusConsumerProperties create(Environment environment, String prefix) {
        var topic = environment.getRequiredProperty(prefix + ".topic");
        if (topic.isBlank()) {
            throw new IllegalStateException(prefix + ".topic can not be blank");
        }
        var groupName = environment.getRequiredProperty(prefix + ".groupName");
        if (groupName.isBlank()) {
            throw new IllegalStateException(prefix + ".groupName can not be blank");
        }
        var groupVersion = Integer.parseInt(environment.getRequiredProperty(prefix + ".groupVersion"));
        if (groupVersion < 1) {
            throw new IllegalStateException(prefix + ".groupVersion can not be less than 1");
        }
        return new LiiklusConsumerProperties(topic, groupName, groupVersion);
    }
}
