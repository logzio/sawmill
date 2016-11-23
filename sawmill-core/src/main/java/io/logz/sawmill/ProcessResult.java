package io.logz.sawmill;

public class ProcessResult {
    private final boolean succeeded;
    private final String errorMessage;

    public ProcessResult(boolean succeeded) {
        this(succeeded, "");
    }

    public ProcessResult(boolean succeeded, String errorMessage) {
        this.succeeded = succeeded;
        this.errorMessage = errorMessage;
    }

    public boolean isSucceeded() {
        return succeeded;
    }

    public String getErrorMessage() {
        return errorMessage;
    }
}
