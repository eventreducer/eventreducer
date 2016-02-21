package org.eventreducer;

import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.net.ntp.TimeStamp;

import java.util.UUID;
/**
 * Event is a statement of a fact that has occurred once written to a journal.
 */
public abstract class Event extends Serializable implements Identifiable {


    @Setter @Accessors(fluent = true)
    private UUID uuid;

    public UUID uuid() {
        if (uuid == null) {
            uuid = UUID.randomUUID();
        }
        return uuid;
    }

    @Getter @Setter @Accessors(fluent = true)
    private TimeStamp timestamp;

    @Getter @Setter @Accessors(fluent = true)
    private Event event;

    @Getter @Setter @Accessors(fluent = true)
    private Command command;

}
