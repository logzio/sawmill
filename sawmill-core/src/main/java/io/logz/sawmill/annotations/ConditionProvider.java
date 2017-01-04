package io.logz.sawmill.annotations;

import io.logz.sawmill.Condition;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface ConditionProvider {
    String type();
    Class<? extends Condition.Factory> factory();
}

