package services.vivy.liiklus;

import com.github.bsideup.liiklus.LiiklusClient;
import com.github.bsideup.liiklus.protocol.PublishReply;
import com.github.bsideup.liiklus.protocol.PublishRequest;
import com.google.protobuf.ByteString;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;

@RequiredArgsConstructor
@FieldDefaults(makeFinal = true)
public class LiiklusPublisher {

    LiiklusClient liiklusClient;

    String topic;

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
