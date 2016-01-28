package org.eventreducer;

import lombok.Getter;

/**
 * This exception wraps exceptions happened during command journalling step
 * and include the original command for reference
 */
public class EventJournallingException extends Exception {

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

    public EventJournallingException(Throwable throwable, Command command) {
        this.throwable = throwable;
        this.command = command;
    }

    @Override
    public String getMessage() {
        return "Exception " + throwable.getClass() + " during command " + command + " journalling: " + throwable.getMessage();
    }

    @Override
    public StackTraceElement[] getStackTrace() {
        return throwable.getStackTrace();
    }
}
