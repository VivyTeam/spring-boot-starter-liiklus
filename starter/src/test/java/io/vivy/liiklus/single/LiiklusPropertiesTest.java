package io.vivy.liiklus.single;

import io.vivy.liiklus.LiiklusProperties;
import io.vivy.liiklus.LiiklusProperties.Target;
import org.junit.jupiter.api.Test;
import org.springframework.validation.BeanPropertyBindingResult;

import java.net.URI;

import static org.assertj.core.api.Assertions.assertThat;

class LiiklusPropertiesTest {

    @Test
    void shouldAllowTargetAndRead() {
        var props = new LiiklusProperties();
        props.setTarget(URI.create("grpc://host"));
        props.setRead(new Target(URI.create("grpc://host"), null));

        var errors = new BeanPropertyBindingResult(props, "props");

        new LiiklusProperties.LiiklusPropertiesValidator().validate(props, errors);

        assertThat(errors.getAllErrors()).hasSize(0);
    }

    @Test
    void shouldCheckEmpty() {
        var props = new LiiklusProperties();
        var errors = new BeanPropertyBindingResult(props, "props");

        new LiiklusProperties.LiiklusPropertiesValidator().validate(props, errors);

        assertThat(errors.getAllErrors()).hasSize(1);
    }

    @Test
    void shouldAllowOnlyRead() {
        var props = new LiiklusProperties();
        props.setRead(new Target(URI.create("grpc://host"), null));

        var errors = new BeanPropertyBindingResult(props, "props");

        new LiiklusProperties.LiiklusPropertiesValidator().validate(props, errors);

        assertThat(errors.getAllErrors()).hasSize(0);
    }
}