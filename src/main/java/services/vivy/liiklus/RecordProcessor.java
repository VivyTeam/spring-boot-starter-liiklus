package services.vivy.liiklus;

import com.github.bsideup.liiklus.protocol.ReceiveReply;
import reactor.core.publisher.Mono;

import java.util.function.Function;

@FunctionalInterface
public interface RecordProcessor extends Function<ReceiveReply.Record, Mono<Void>> {

}
