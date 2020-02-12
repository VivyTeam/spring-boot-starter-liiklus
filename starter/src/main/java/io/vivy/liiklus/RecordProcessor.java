package io.vivy.liiklus;

import com.github.bsideup.liiklus.protocol.ReceiveReply;
import reactor.core.publisher.Mono;

import java.util.function.Function;

/**
 * Simple version of the {@link io.vivy.liiklus.PartitionAwareProcessor},
 * which doesn't care about partition.
 */
@Deprecated
@FunctionalInterface
public interface RecordProcessor extends Function<ReceiveReply.Record, Mono<Void>> {

}