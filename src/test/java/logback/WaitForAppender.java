package logback;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;

import static com.google.common.base.Preconditions.checkNotNull;

public class WaitForAppender extends AppenderBase<ILoggingEvent> {

    private final String searchString;
    private boolean foundSearchString = false;

    public WaitForAppender(String searchString) {
        this.searchString = checkNotNull(searchString);
    }

    @Override
    protected void append(ILoggingEvent event) {
        if (!foundSearchString) {
            // toString since we want the level as well
            if (event.toString().contains(searchString)) {
                foundSearchString  = true;
            }
        }
    }

    public boolean foundSearchString() {
        return foundSearchString;
    }
}
