package io.logz.sawmill.exceptions;

public class ProcessorConfigurationException extends SawmillException {
    public ProcessorConfigurationException(String errorMsg) {
        super(errorMsg);
    }

    public ProcessorConfigurationException(String errorMsg, Exception e) {
        super(errorMsg, e);
    }
}
