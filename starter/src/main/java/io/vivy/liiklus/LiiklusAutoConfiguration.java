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
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.ReplayProcessor;
import reactor.core.publisher.SignalType;
import reactor.core.scheduler.Schedulers;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

@Configuration
@EnableConfigurationProperties(LiiklusProperties.class)
@ConditionalOnClass({LiiklusClient.class})
@Slf4j
public class LiiklusAutoConfiguration {

    @Autowired
    LiiklusProperties properties;

    @Bean
    LiiklusClient liiklusClient() {
        var targetURI = properties.getTargetURI();

        switch (targetURI.getScheme()) {
            case "rsocket":
                TcpClientTransport transport = TcpClientTransport.create(new InetSocketAddress(targetURI.getHost(), targetURI.getPort()));
                RSocket rSocket = RSocketFactory.connect()
                        .transport(transport)
                        .start()
                        .block();

                return new RSocketLiiklusClient(rSocket);
            default:
                ManagedChannel channel = NettyChannelBuilder.forAddress(targetURI.getHost(), targetURI.getPort())
                        .directExecutor()
                        .usePlaintext()
                        .keepAliveWithoutCalls(true)
                        .keepAliveTime(150, TimeUnit.SECONDS)
                        .build();

                return new GRPCLiiklusClient(channel);
        }
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