package io.vivy.liiklus;

import com.github.bsideup.liiklus.LiiklusClient;
import com.github.bsideup.liiklus.protocol.PublishReply;
import com.github.bsideup.liiklus.protocol.PublishRequest;
import com.google.protobuf.ByteString;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;

@Getter
@RequiredArgsConstructor
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
public class LiiklusPublisher {

    String topic;
    LiiklusClient liiklusClient;

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
