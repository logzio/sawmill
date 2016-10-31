package logback;

import ch.qos.logback.core.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestingAppenderFactory {

    public WaitForAppender createWaitForAppender(Class classToLog, String waitForString) {
        return createWaitForAppender(classToLog.getName(), waitForString);
    }

    public WaitForAppender createWaitForAppender(String loggerName, String waitForString) {
        ch.qos.logback.classic.Logger logbackLogger = getLogbackLogger(loggerName);

        Context logbackContext = logbackLogger.getLoggerContext();
        WaitForAppender waitForAppender = new WaitForAppender(waitForString);
        waitForAppender.setContext(logbackContext);
        waitForAppender.start();

        logbackLogger.addAppender(waitForAppender);
        logbackLogger.setAdditive(true);

        return waitForAppender;
    }

    public void removeWaitForAppender(Class classToLog, WaitForAppender appender) {
        removeWaitForAppender(classToLog.getName(), appender);
    }

    public void removeWaitForAppender(String loggerName, WaitForAppender appender) {
        ch.qos.logback.classic.Logger logbackLogger = getLogbackLogger(loggerName);
        logbackLogger.detachAppender(appender);
    }

    private ch.qos.logback.classic.Logger getLogbackLogger(String loggerName) {
        Logger logger = LoggerFactory.getLogger(loggerName);
        if (!(logger instanceof ch.qos.logback.classic.Logger)) {
            throw new IllegalStateException("Only support logback");
        }
        return (ch.qos.logback.classic.Logger) logger;
    }
}
