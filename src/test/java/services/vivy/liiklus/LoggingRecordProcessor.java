package services.vivy.liiklus;

import com.github.bsideup.liiklus.protocol.ReceiveReply;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
@Slf4j
public class LoggingRecordProcessor implements RecordProcessor {

    @Override
    public Mono<Void> apply(ReceiveReply.Record record) {
        log.info("Received record: {}", record);
        return Mono.empty();
    }
}
