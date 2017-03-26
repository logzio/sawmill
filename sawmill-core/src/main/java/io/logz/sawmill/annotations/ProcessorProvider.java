package io.logz.sawmill.annotations;

import io.logz.sawmill.Processor;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface ProcessorProvider {
    String type();
    Class<? extends Processor.Factory> factory();
}
