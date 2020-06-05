package io.vivy.liiklus.support;

import com.github.bsideup.liiklus.LiiklusClient;
import com.github.bsideup.liiklus.protocol.LiiklusEvent;
import com.github.bsideup.liiklus.protocol.PublishReply;
import com.github.bsideup.liiklus.protocol.PublishRequest;
import com.google.protobuf.ByteString;
import io.vivy.liiklus.LiiklusPublisher;
import reactor.core.publisher.Mono;

import java.util.UUID;

/**
 * Publishes both value and liiklus event at the same time
 */
public class TestLiiklusCloudEventPublisher extends LiiklusPublisher {

    private String topic;
    private LiiklusClient liiklusClient;

    public TestLiiklusCloudEventPublisher(String topic, LiiklusClient liiklusClient) {
        super(topic, liiklusClient);
        this.topic = topic;
        this.liiklusClient = liiklusClient;
    }

    @Override
    public Mono<PublishReply> publish(String key, byte[] value) {
        return liiklusClient.publish(
                PublishRequest.newBuilder()
                        .setKey(ByteString.copyFromUtf8(key))
                        .setTopic(topic)
                        .setLiiklusEvent(LiiklusEvent.newBuilder()
                                .setId(UUID.randomUUID().toString())
                                .setType("com.vivy.events.test.event")
                                .setSource("/test")
                                .setData(ByteString.copyFrom(value))
                                .build()
                        )
                        .build()
        );
    }
}
