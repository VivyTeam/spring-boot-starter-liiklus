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
        var groupName = environment.getRequiredProperty(prefix + ".groupName");
        var groupVersion = environment.getRequiredProperty(prefix + ".groupVersion");
        return new LiiklusConsumerProperties(topic, groupName, Integer.parseInt(groupVersion));
    }
}
