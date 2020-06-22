package io.logz.sawmill.exceptions;

public class FactoryInstantiationException extends SawmillException {

    public FactoryInstantiationException(String errorMsg) {
        super(errorMsg);
    }
    public FactoryInstantiationException(String errorMsg, Exception e) {
        super(errorMsg, e);
    }
}
