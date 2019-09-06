package io.vivy.liiklus;

import com.github.bsideup.liiklus.GRPCLiiklusClient;
import com.github.bsideup.liiklus.LiiklusClient;
import com.github.bsideup.liiklus.RSocketLiiklusClient;
import com.github.bsideup.liiklus.protocol.AckRequest;
import com.github.bsideup.liiklus.protocol.GetOffsetsReply;
import com.github.bsideup.liiklus.protocol.GetOffsetsRequest;
import com.github.bsideup.liiklus.protocol.PublishReply;
import com.github.bsideup.liiklus.protocol.PublishRequest;
import com.github.bsideup.liiklus.protocol.ReceiveReply;
import com.github.bsideup.liiklus.protocol.ReceiveRequest;
import com.github.bsideup.liiklus.protocol.SubscribeReply;
import com.github.bsideup.liiklus.protocol.SubscribeRequest;
import com.google.protobuf.Empty;
import io.grpc.ManagedChannel;
import io.grpc.netty.NettyChannelBuilder;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.transport.netty.client.TcpClientTransport;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.validation.Validator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.ReplayProcessor;
import reactor.core.publisher.SignalType;
import reactor.core.scheduler.Schedulers;

import java.net.InetSocketAddress;
import java.net.URI;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

@Configuration
@EnableConfigurationProperties(LiiklusProperties.class)
@ConditionalOnClass({LiiklusClient.class})
@Slf4j
public class LiiklusAutoConfiguration {

    private static final String RSOCKET_TRANSPORT = "rsocket";

    @Autowired
    LiiklusProperties properties;

    private static LiiklusClient createClient(URI targetURI) {
        switch (targetURI.getScheme()) {
            case RSOCKET_TRANSPORT:
                return rsocket(targetURI);
            default:
                return grpc(targetURI);
        }
    }

    private static LiiklusClient rsocket(URI targetURI) {
        TcpClientTransport transport = TcpClientTransport.create(new InetSocketAddress(targetURI.getHost(), targetURI.getPort()));
        RSocket rSocket = RSocketFactory.connect()
                .transport(transport)
                .start()
                .block();

        return new RSocketLiiklusClient(rSocket);
    }

    private static LiiklusClient grpc(URI targetURI) {
        ManagedChannel channel = NettyChannelBuilder.forAddress(targetURI.getHost(), targetURI.getPort())
                .directExecutor()
                .usePlaintext()
                .keepAliveWithoutCalls(true)
                .keepAliveTime(150, TimeUnit.SECONDS)
                .build();

        return new GRPCLiiklusClient(channel);
    }


    @Bean
    LiiklusClient liiklusClient() {
        var read = properties.getRead() == null ? properties.getTarget() : properties.getRead().getUri();
        var write = properties.getWrite() == null ? properties.getTarget() : properties.getWrite().getUri();

        var readClient = createClient(read);
        var writeClient = createClient(write);

        return new LiiklusClient() {
            @Override
            public Mono<PublishReply> publish(PublishRequest message) {
                return writeClient.publish(message);
            }

            @Override
            public Flux<SubscribeReply> subscribe(SubscribeRequest message) {
                return readClient.subscribe(message);
            }

            @Override
            public Flux<ReceiveReply> receive(ReceiveRequest message) {
                return readClient.receive(message);
            }

            @Override
            public Mono<Empty> ack(AckRequest message) {
                return readClient.ack(message);
            }

            @Override
            public Mono<GetOffsetsReply> getOffsets(GetOffsetsRequest message) {
                return readClient.getOffsets(message);
            }
        };
    }

    @Bean
    public static Validator configurationPropertiesValidator() {
        return new LiiklusProperties.LiiklusPropertiesValidator();
    }

    @Bean
    LiiklusPublisher liiklusPublisher(LiiklusClient liiklusClient) {
        return new LiiklusPublisher(liiklusClient, properties.getTopic());
    }

    @Bean
    @ConditionalOnBean(PartitionAwareProcessor.class)
    ApplicationRunner partitionAwareLiiklusLoop(LiiklusClient liiklusClient, PartitionAwareProcessor partitionAwareProcessor) {
        return createLiiklusRunner(liiklusClient, partitionAwareProcessor);
    }

    @Bean
    @ConditionalOnBean(RecordProcessor.class)
    @ConditionalOnMissingBean(PartitionAwareProcessor.class)
    ApplicationRunner partitionUnawareLiiklusLoop(LiiklusClient liiklusClient, RecordProcessor recordProcessor) {
        return createLiiklusRunner(liiklusClient, (__, record) -> recordProcessor.apply(record));
    }

    private ApplicationRunner createLiiklusRunner(
            LiiklusClient liiklusClient,
            PartitionAwareProcessor partitionAwareProcessor
    ) {
        var ackScheduler = Schedulers.newSingle("ack");
        return new LiiklusConsumerLoop(
                liiklusClient,
                properties,
                (partition, records) -> {
                    var ackInProgress = ReplayProcessor.cacheLastOrDefault(false);
                    var ackFinished = ackInProgress.filter(Boolean.FALSE::equals);

                    return records
                            .concatMap(
                                    record -> Mono.defer(() -> partitionAwareProcessor.apply(partition, record))
                                            .log("processor", Level.SEVERE, SignalType.ON_ERROR)
                                            .retryWhen(it -> it.delayElements(Duration.ofSeconds(1)))
                                            .delaySubscription(ackFinished)
                                            .thenReturn(record.getOffset()),
                                    1_000
                            )
                            .sample(Flux.interval(properties.getAckInterval(), ackScheduler))

                            .doOnNext(__ -> ackInProgress.onNext(true))
                            .doOnRequest(__ -> Schedulers.parallel().schedule(() -> ackInProgress.onNext(false)));
                }
        );
    }

}