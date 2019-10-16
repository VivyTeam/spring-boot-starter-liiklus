package io.vivy.liiklus;

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import com.avast.grpc.jwt.client.JwtCallCredentials;
import com.github.bsideup.liiklus.GRPCLiiklusClient;
import com.github.bsideup.liiklus.LiiklusClient;
import com.github.bsideup.liiklus.RSocketLiiklusClient;
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
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.MethodDescriptor;
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
import org.springframework.validation.Validator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.ReplayProcessor;
import reactor.core.publisher.SignalType;
import reactor.core.scheduler.Schedulers;

import java.net.InetSocketAddress;
import java.time.Clock;
import java.time.Duration;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

@Configuration
@EnableConfigurationProperties(LiiklusProperties.class)
@ConditionalOnClass({LiiklusClient.class})
@Slf4j
public class LiiklusAutoConfiguration {

    private static final String RSOCKET_TRANSPORT = "rsocket";

    private static final Duration TOKEN_EXPIRE_TIME = Duration.ofMinutes(1);

    private static final String LIIKLUS_CLOCK = "liiklus_clock";

    @Autowired
    LiiklusProperties properties;

    private LiiklusClient createClient(LiiklusProperties.Target target, Clock clock) {
        if (target.getUri() == null) {
            return new ComplainingLiiklusClient();
        }

        switch (target.getUri().getScheme()) {
            case RSOCKET_TRANSPORT:
                return rsocket(target);
            default:
                return grpc(target, clock);
        }
    }

    private LiiklusClient rsocket(LiiklusProperties.Target target) {
        TcpClientTransport transport = TcpClientTransport.create(new InetSocketAddress(target.getUri().getHost(), target.getUri().getPort()));
        RSocket rSocket = RSocketFactory.connect()
                .transport(transport)
                .start()
                .block();

        return new RSocketLiiklusClient(rSocket);
    }

    private LiiklusClient grpc(LiiklusProperties.Target target, Clock clock) {
        var builder = NettyChannelBuilder.forAddress(target.getUri().getHost(), target.getUri().getPort())
                .directExecutor()
                .usePlaintext()
                .keepAliveWithoutCalls(true)
                .keepAliveTime(150, TimeUnit.SECONDS);

        if (target.getSecret() != null) {
            var algorithm = Algorithm.HMAC512(target.getSecret());

            builder.intercept(new ClientInterceptor() {
                @Override
                public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> call, CallOptions headers, Channel next) {
                    return next.newCall(call, headers.withCallCredentials(JwtCallCredentials.blocking(() -> JWT
                            .create()
                            .withExpiresAt(Date.from(clock.instant().plus(TOKEN_EXPIRE_TIME)))
                            .sign(algorithm)
                    )));
                }
            });
        }

        return new GRPCLiiklusClient(builder.build());
    }

    @Bean(LIIKLUS_CLOCK)
    Clock clock() {
        return Clock.systemUTC();
    }

    @Bean
    LiiklusClient liiklusClient(@Qualifier(LIIKLUS_CLOCK) Clock clock) {
        var defaultTarget = new LiiklusProperties.Target(properties.getTarget(), null);
        var read = properties.getRead() == null ? defaultTarget : properties.getRead();
        var write = properties.getWrite() == null ? defaultTarget : properties.getWrite();

        var readClient = createClient(read, clock);
        var writeClient = createClient(write, clock);

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

            @Override
            public Mono<GetEndOffsetsReply> getEndOffsets(GetEndOffsetsRequest message) {
                return readClient.getEndOffsets(message);
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

    private static class ComplainingLiiklusClient implements LiiklusClient {
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
}