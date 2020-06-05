package io.vivy.liiklus.producer;

import com.github.bsideup.liiklus.LiiklusClient;
import com.github.bsideup.liiklus.protocol.PublishReply;
import com.github.bsideup.liiklus.protocol.PublishRequest;
import com.google.protobuf.ByteString;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.experimental.FieldDefaults;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;

@Getter
@FieldDefaults(level = AccessLevel.PRIVATE)
public class LiiklusProducer {

    String topic;

    LiiklusClient liiklusClient;

    public void init(LiiklusClient liiklusClient, String topic) {
        this.liiklusClient = liiklusClient;
        this.topic = topic;
    }

    public Mono<PublishReply> publish(String key, ByteBuffer value) {
        return publish(key, value.array());
    }

    public Mono<PublishReply> publish(String key, byte[] value) {
        PublishRequest publishRequest = PublishRequest.newBuilder()
                .setTopic(topic)
                .setKey(ByteString.copyFromUtf8(key))
                .setValue(ByteString.copyFrom(value))
                .build();

        return liiklusClient.publish(publishRequest)
                .checkpoint("publish-" + key);
    }
}
