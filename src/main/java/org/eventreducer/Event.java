package org.eventreducer;

import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.util.UUID;
/**
 * Event is a statement of a fact that has occurred once written to a journal.
 */
public abstract class Event extends Serializable {

    @Getter @Setter @Accessors(fluent = true)
    private UUID uuid = UUID.randomUUID();

    @Getter @Setter @Accessors(fluent = true)
    private long timestamp;

    @Getter @Setter @Accessors(fluent = true)
    private Event event;

    @Getter @Setter @Accessors(fluent = true)
    private Command command;

    public void onEventJournaled(Endpoint storage) {}

}
