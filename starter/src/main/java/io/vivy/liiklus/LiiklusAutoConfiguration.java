package io.vivy.liiklus;

import com.github.bsideup.liiklus.GRPCLiiklusClient;
import com.github.bsideup.liiklus.LiiklusClient;
import com.github.bsideup.liiklus.RSocketLiiklusClient;
import io.grpc.ManagedChannel;
import io.grpc.netty.NettyChannelBuilder;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.transport.netty.client.TcpClientTransport;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
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

    private static final String READ_LIIKLUS_CLIENT_QUALIFIER = "readLiiklusClient";

    private static final String WRITE_LIIKLUS_CLIENT_QUALIFIER = "writeLiiklusClient";

    private static final String RSOCKET_TRANSPORT = "rsocket";

    @Autowired
    LiiklusProperties properties;

    @Bean
    @Primary
    @Qualifier(READ_LIIKLUS_CLIENT_QUALIFIER)
    LiiklusClient readLiiklusClient() {
        var targetURI = properties.getReadUri();

        switch (targetURI.getScheme()) {
            case RSOCKET_TRANSPORT:
                return rsocket(targetURI);
            default:
                return grpc(targetURI);
        }
    }

    @Bean
    @Qualifier(WRITE_LIIKLUS_CLIENT_QUALIFIER)
    LiiklusClient writeLiiklusClient() {
        var targetURI = properties.getWriteUri();

        switch (targetURI.getScheme()) {
            case RSOCKET_TRANSPORT:
                return rsocket(targetURI);
            default:
                return grpc(targetURI);
        }
    }

    @Bean
    public static Validator configurationPropertiesValidator() {
        return new LiiklusProperties.LiiklusPropertiesValidator();
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
    @ConditionalOnBean(name = WRITE_LIIKLUS_CLIENT_QUALIFIER, value = LiiklusClient.class)
    LiiklusPublisher liiklusPublisher(@Qualifier(WRITE_LIIKLUS_CLIENT_QUALIFIER) LiiklusClient writeLiiklusClient) {
        return new LiiklusPublisher(writeLiiklusClient, properties.getTopic());
    }

    @Bean
    @ConditionalOnBean(PartitionAwareProcessor.class)
    ApplicationRunner partitionAwareLiiklusLoop(@Qualifier(READ_LIIKLUS_CLIENT_QUALIFIER) LiiklusClient readLiiklusClient, PartitionAwareProcessor partitionAwareProcessor) {
        return createLiiklusRunner(readLiiklusClient, partitionAwareProcessor);
    }

    @Bean
    @ConditionalOnBean(RecordProcessor.class)
    @ConditionalOnMissingBean(PartitionAwareProcessor.class)
    ApplicationRunner partitionUnawareLiiklusLoop(@Qualifier(READ_LIIKLUS_CLIENT_QUALIFIER) LiiklusClient readLiiklusClient, RecordProcessor recordProcessor) {
        return createLiiklusRunner(readLiiklusClient, (__, record) -> recordProcessor.apply(record));
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