# Liiklus starter

## Usage guide

Add to dependency management

`dependency 'com.github.UvitaTeam.spring-boot-starter-liiklus:starter:$latest'`

and to module 

`compile 'com.github.UvitaTeam.spring-boot-starter-liiklus:starter'`


define properties to connect to liiklus

```java
liiklus.target=grpc://liiklus:8080
liiklus.topic=topic
liiklus.groupName=group
liiklus.groupVersion=2

// optionally in boot 2 notation
liiklus.ackInterval=10ms // default is 5s
```

### To consume the messages
implement `io.vivy.liiklus.RecordProcessor`

usually that's the parsing of the raw bytes and delegation to the real processor of the messages

```java
@Configuration
@Slf4j
public class LiiklusConfiguration {

    @Autowired
    ObjectMapper objectMapper;

    @Bean
    RecordProcessor recordProcessor(EventLogProcessor<UserEvent> eventLogProcessor) {
        ObjectMapper flexibleObjectMapper = objectMapper.copy()
                // in case you don't want to fail message processing on parse
                .disable(DeserializationFeature.FAIL_ON_INVALID_SUBTYPE)
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

        return record -> {
            final UserEvent userEvent;
            try {
                userEvent = flexibleObjectMapper.readValue(record.getValue().toByteArray(), UserEvent.class);
            } catch (Exception e) {
                ContextLogger.of(log).event("main_consume_loop")
                        .with("status", "json_read_error")
                        .error(record.getValue().toStringUtf8(), e);
                return Mono.never();
            }

            return eventLogProcessor
                    .apply(
                            new EventLogProcessor.Event<>() {

                                @Override
                                public boolean isReplay() {
                                    return record.getReplay();
                                }

                                @Getter(lazy = true)
                                private final String key = record.getKey().toStringUtf8();

                                @Getter
                                private final long offset = record.getOffset();

                                @Getter
                                private final UserEvent value = userEvent;

                                @Getter(lazy = true)
                                private final Instant timestamp = Instant.ofEpochMilli(Timestamps.toMillis(record.getTimestamp()));
                            }
                    )
                    .log("processor", Level.SEVERE, SignalType.ON_ERROR)
                    .then();
        };
    }
}
```

### To produce the messages

Just inject `io.vivy.liiklus.LiiklusPublisher` and use `Mono<PublishReply> publish(String key, byte[] value)` method
