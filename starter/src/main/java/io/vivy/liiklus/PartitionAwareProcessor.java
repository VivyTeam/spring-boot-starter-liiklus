package io.vivy.liiklus;

import com.github.bsideup.liiklus.protocol.ReceiveReply;
import reactor.core.publisher.Mono;

import java.util.function.BiFunction;

@Deprecated
@FunctionalInterface
public interface PartitionAwareProcessor extends BiFunction<Integer, ReceiveReply.Record, Mono<Void>> {

}