package io.logz.sawmill.executor.annotations;

import io.logz.sawmill.executor.Input;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface InputProvider {
    String type();
    Class<? extends Input.Factory> factory();
}
