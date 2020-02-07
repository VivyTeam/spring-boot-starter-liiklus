package io.vivy.liiklus.support;

import com.github.bsideup.liiklus.LiiklusClient;
import com.github.bsideup.liiklus.protocol.AckRequest;
import com.github.bsideup.liiklus.protocol.GetEndOffsetsReply;
import com.github.bsideup.liiklus.protocol.GetEndOffsetsRequest;
import com.github.bsideup.liiklus.protocol.GetOffsetsReply;
import com.github.bsideup.liiklus.protocol.GetOffsetsRequest;
import com.github.bsideup.liiklus.protocol.PublishReply;
import com.github.bsideup.liiklus.protocol.PublishRequest;
import com.github.bsideup.liiklus.protocol.ReceiveReply;
import com.github.bsideup.liiklus.protocol.ReceiveRequest;
import com.github.bsideup.liiklus.protocol.SubscribeReply;
import com.github.bsideup.liiklus.protocol.SubscribeRequest;
import com.google.protobuf.Empty;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class ComplainingLiiklusClient implements LiiklusClient {
    @Override
    public Mono<PublishReply> publish(PublishRequest message) {
        return Mono.error(new IllegalCallerException("liiklus write target is undefined"));
    }

    @Override
    public Flux<SubscribeReply> subscribe(SubscribeRequest message) {
        return Flux.error(new IllegalCallerException("liiklus read target is undefined"));
    }

    @Override
    public Flux<ReceiveReply> receive(ReceiveRequest message) {
        return Flux.error(new IllegalCallerException("liiklus read target is undefined"));
    }

    @Override
    public Mono<Empty> ack(AckRequest message) {
        return Mono.error(new IllegalCallerException("liiklus read target is undefined"));
    }

    @Override
    public Mono<GetOffsetsReply> getOffsets(GetOffsetsRequest message) {
        return Mono.error(new IllegalCallerException("liiklus read target is undefined"));
    }

    @Override
    public Mono<GetEndOffsetsReply> getEndOffsets(GetEndOffsetsRequest message) {
        return Mono.error(new IllegalCallerException("liiklus read target is undefined"));
    }
}
