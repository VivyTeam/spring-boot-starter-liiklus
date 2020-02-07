package io.vivy.liiklus.common;

import org.springframework.core.annotation.AnnotationUtils;

import static java.util.Objects.isNull;

public class LiiklusUtils {

    public static String getLiiklusPrefix(Object object) {
        Liiklus annotation = AnnotationUtils.findAnnotation(object.getClass(), Liiklus.class);
        if (isNull(annotation)) {
            throw new IllegalArgumentException("The classes must be annotated with @Liiklus");
        }
        return annotation.prefix();
    }
}
