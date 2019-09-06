package io.vivy.liiklus;

import org.junit.jupiter.api.Test;
import org.springframework.validation.BeanPropertyBindingResult;

import java.net.URI;

import static org.assertj.core.api.Assertions.assertThat;

class LiiklusPropertiesTest {

    @Test
    void shouldExcludeTargetAndRead() {
        var props = new LiiklusProperties();
        props.setTarget(URI.create("grpc://host"));
        props.setRead(new LiiklusProperties.Target(URI.create("grpc://host")));

        var errors = new BeanPropertyBindingResult(props, "props");

        new LiiklusProperties.LiiklusPropertiesValidator().validate(props, errors);

        assertThat(errors.getAllErrors()).hasSize(1);
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
        props.setRead(new LiiklusProperties.Target(URI.create("grpc://host")));

        var errors = new BeanPropertyBindingResult(props, "props");

        new LiiklusProperties.LiiklusPropertiesValidator().validate(props, errors);

        assertThat(errors.getAllErrors()).hasSize(0);
    }
}