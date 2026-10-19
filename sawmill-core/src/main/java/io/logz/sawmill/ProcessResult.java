package io.logz.sawmill;

import com.google.common.base.Suppliers;
import io.logz.sawmill.exceptions.ProcessorExecutionException;

import java.util.Optional;
import java.util.function.Supplier;

import static io.logz.sawmill.Result.DROPPED;
import static io.logz.sawmill.Result.FAILED;
import static io.logz.sawmill.Result.SUCCEEDED;

public class ProcessResult {
    private final Result result;
    private final Optional<Error> error;

    private static ProcessResult processSucceeded = new ProcessResult();
    private static ProcessResult processDropped = new ProcessResult(DROPPED);

    private ProcessResult() {
        this(SUCCEEDED);
    }

    private ProcessResult(Result result) {
        this.result = result;
        this.error = Optional.empty();
    }

    private ProcessResult(String errorMessage) {
        this(errorMessage, null);
    }

    private ProcessResult(String errorMessage, ProcessorExecutionException e) {
        this.result = FAILED;
        this.error = Optional.of(new Error(errorMessage, Optional.ofNullable(e)));
    }

    private ProcessResult(Supplier<String> errorMessage) {
        this.result = FAILED;
        this.error = Optional.of(new Error(errorMessage, Optional.empty()));
    }

    public boolean isSucceeded() {
        return result == SUCCEEDED;
    }

    public boolean isDropped() {
        return result == DROPPED;
    }

    public Optional<Error> getError() {
        return error;
    }

    public static ProcessResult success() {
        return processSucceeded;
    }

    public static ProcessResult failure(String errorMessage) {
        return new ProcessResult(errorMessage);
    }

    /**
     * The message is built only if it is read. Most failures are handled by onFailure steps or dropped when the
     * pipeline continues, so their messages are never read.
     * <p>
     * The supplier runs on the first {@link Error#getMessage()}, which may come after later processors changed the
     * doc. Capture only values that will not change: strings, the processor's configuration, or doc values rendered
     * to a string when the failure is created.
     */
    public static ProcessResult failure(Supplier<String> errorMessage) {
        return new ProcessResult(errorMessage);
    }

    public static ProcessResult failure(String errorMessage, ProcessorExecutionException e) {
        return new ProcessResult(errorMessage, e);
    }

    public static ProcessResult drop() {
        return processDropped;
    }

    public static class Error {
        private final Supplier<String> message;
        private final Optional<ProcessorExecutionException> exception;

        public Error(String message, Optional<ProcessorExecutionException> exception) {
            this.message = () -> message;
            this.exception = exception;
        }

        private Error(Supplier<String> message, Optional<ProcessorExecutionException> exception) {
            this.message = Suppliers.memoize(message::get);
            this.exception = exception;
        }

        public String getMessage() {
            return message.get();
        }

        public Optional<ProcessorExecutionException> getException() {
            return exception;
        }
    }
}
