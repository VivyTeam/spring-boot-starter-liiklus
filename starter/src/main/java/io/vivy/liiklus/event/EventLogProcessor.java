package io.vivy.liiklus.event;

import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.function.Function;

public interface EventLogProcessor<T> extends Function<EventLogProcessor.Event<T>, Mono<?>> {

    interface Event<T> {

        int getPartition();

        long getOffset();

        String getKey();

        T getValue();

        Instant getTimestamp();

        boolean isReplay();
    }
}
