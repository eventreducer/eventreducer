package org.eventreducer;

import lombok.Getter;

/**
 * This exception wraps exceptions happened during event extraction step
 * and include the original command for reference
 */
public class EventExtractionException extends Exception {

    /**
     * Original exception
     */
    @Getter
    private final Throwable throwable;
    /**
     * Command that was being processed when the exception occurred
     */
    @Getter
    private final Command command;

    public EventExtractionException(Throwable throwable, Command command) {
        this.throwable = throwable;
        this.command = command;
    }

    @Override
    public String getMessage() {
        return "Exception " + throwable.getClass() + " during command " + command + " event extraction: " + throwable.getMessage();
    }

    @Override
    public StackTraceElement[] getStackTrace() {
        return throwable.getStackTrace();
    }
}
