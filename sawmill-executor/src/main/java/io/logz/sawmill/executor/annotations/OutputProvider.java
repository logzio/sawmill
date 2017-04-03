package io.logz.sawmill.executor.annotations;

import io.logz.sawmill.executor.Output;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface OutputProvider {
    String type();
    Class<? extends Output.Factory> factory();
}
