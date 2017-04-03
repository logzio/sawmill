package io.logz.sawmill.executor.exceptions;

import io.logz.sawmill.exceptions.SawmillException;

public class OutputMissingException extends SawmillException {
    public OutputMissingException(String errorMsg) {
        super(errorMsg);
    }
}
