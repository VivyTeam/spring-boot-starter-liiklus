package io.vivy.liiklus.consumer;

import io.vivy.liiklus.LiiklusProperties;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;

import static java.util.Objects.isNull;

@RequiredArgsConstructor
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
@Getter
public class LiiklusConsumerProperties {

    private static final int DEFAULT_GROUP_VERSION = 1;

    String topic;
    String groupName;
    int groupVersion;

    public static LiiklusConsumerProperties create(LiiklusProperties properties, String name) {
        var topic = properties.getTopics().get(name + ".topic");
        if (isNull(topic) || topic.isBlank()) {
            throw new IllegalStateException(name + ".topic can not be null or blank");
        }
        var groupName = properties.getTopics().get(name + ".groupName");
        if (isNull(groupName) || groupName.isBlank()) {
            throw new IllegalStateException(name + ".groupName can not be null or blank");
        }
        var groupVersionString = properties.getTopics().get(name + ".groupVersion");
        var groupVersion = isNull(groupVersionString) ? DEFAULT_GROUP_VERSION : Integer.parseInt(groupVersionString);
        if (groupVersion < 1) {
            throw new IllegalStateException(name + ".groupVersion can not be less than 1");
        }
        return new LiiklusConsumerProperties(topic, groupName, groupVersion);
    }
}
