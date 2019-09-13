package io.vivy.liiklus.auth.grpc;

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;

import java.time.Clock;
import java.time.Duration;
import java.util.Date;

@FieldDefaults(makeFinal = true)
@Slf4j
public class JWTClientInterceptor implements ClientInterceptor {

    public static final String AUTHENTICATION_HEADER = "authentication";
    public static final Duration EXPIRY = Duration.ofMinutes(2);

    Algorithm algorithm;

    Clock clock;

    public JWTClientInterceptor(Clock clock, String secret) {
        this.clock = clock;
        algorithm = Algorithm.HMAC512(secret);
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
            MethodDescriptor<ReqT, RespT> method,
            CallOptions callOptions,
            Channel next
    ) {
        return new ForwardingClientCall.SimpleForwardingClientCall<>(next.newCall(method, callOptions)) {
            @Override
            public void start(Listener<RespT> responseListener, Metadata headers) {
                log.info("ADDING AUTHENTICATION...");
                headers.put(Metadata.Key.of(AUTHENTICATION_HEADER, Metadata.ASCII_STRING_MARSHALLER), createAuthenticationHeader());
                super.start(responseListener, headers);
            }
        };
    }

    private String createAuthenticationHeader() {
        return "Bearer " + JWT
                .create()
                .withExpiresAt(Date.from(clock.instant().plus(EXPIRY)))
                .sign(algorithm);
    }
}
