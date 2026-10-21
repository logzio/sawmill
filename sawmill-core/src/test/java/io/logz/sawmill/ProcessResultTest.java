package io.logz.sawmill;

import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

public class ProcessResultTest {

    @Test
    public void testLazyMessageIsBuiltOnlyWhenRead() {
        AtomicInteger built = new AtomicInteger();
        ProcessResult result = ProcessResult.failure(() -> {
            built.incrementAndGet();
            return "failed to do [x]";
        });

        assertThat(result.isSucceeded()).isFalse();
        assertThat(built.get()).isEqualTo(0);
        assertThat(result.getError().get().getMessage()).isEqualTo("failed to do [x]");
        assertThat(result.getError().get().getMessage()).isEqualTo("failed to do [x]");
        assertThat(built.get()).isEqualTo(1);
    }

    @Test
    public void testLazyExecutionResultMessage() {
        AtomicInteger built = new AtomicInteger();
        ExecutionResult result = ExecutionResult.failure(() -> {
            built.incrementAndGet();
            return "failed";
        }, "processor");

        assertThat(result.isFailed()).isTrue();
        assertThat(built.get()).isEqualTo(0);
        assertThat(result.getError().get().getMessage()).isEqualTo("failed");
        assertThat(result.getError().get().getFailedProcessorName()).isEqualTo("processor");
        assertThat(built.get()).isEqualTo(1);
    }
}
