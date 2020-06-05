package io.vivy.liiklus;

import com.github.bsideup.liiklus.LiiklusClient;
import com.github.bsideup.liiklus.protocol.PublishReply;
import io.vivy.liiklus.producer.LiiklusProducer;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.experimental.FieldDefaults;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;

/**
 * @deprecated replaced with LiiklusProducer,
 * but would be redesigned to support multiple topics in one bean
 */
@Getter
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
@Deprecated
public class LiiklusPublisher {

    LiiklusProducer liiklusProducer;

    public LiiklusPublisher(String topic, LiiklusClient liiklusClient) {
        this.liiklusProducer = new LiiklusProducer();
        liiklusProducer.init(liiklusClient, topic);
    }

    public Mono<PublishReply> publish(String key, ByteBuffer value) {
        return liiklusProducer.publish(key, value.array());
    }

    public Mono<PublishReply> publish(String key, byte[] value) {
        return liiklusProducer.publish(key, value);
    }
}
