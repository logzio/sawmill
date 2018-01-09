package io.logz.sawmill.executor.exceptions;

import io.logz.sawmill.exceptions.SawmillException;

public class InputMissingException extends SawmillException {
    public InputMissingException(String errorMsg) {
        super(errorMsg);
    }
}
