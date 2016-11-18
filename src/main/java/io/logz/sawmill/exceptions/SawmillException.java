package io.logz.sawmill.exceptions;

public class SawmillException extends RuntimeException {
    public SawmillException(String errorMsg) {
        super(errorMsg);
    }
    public SawmillException(String errorMsg, Exception e) {
        super(errorMsg, e);
    }

}
