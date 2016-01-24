package org.eventreducer;

import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.net.ntp.TimeStamp;
import org.eventreducer.hlc.HybridTimestamp;
import org.eventreducer.hlc.PhysicalTimeProvider;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public abstract class Journal implements EndpointComponent {


    @Getter @Setter @Accessors(fluent = true)
    private Endpoint endpoint;
    private HybridTimestamp timestamp;

    public Journal(PhysicalTimeProvider physicalTimeProvider) {
        this.timestamp = new HybridTimestamp(physicalTimeProvider);
    }

    public void save(Command command, List<Event> events) throws Exception {
        TimeStamp commandTimestamp = new TimeStamp(timestamp.update());
        command.timestamp(commandTimestamp);
        events.stream().
                forEachOrdered(event -> {
                    event.
                            command(command).
                            timestamp(new TimeStamp(timestamp.update()));
                });

        if (!(command instanceof EphemeralCommand)) {
            journal(command, events);
        }
        events.stream().forEachOrdered(event -> {
            event.onEventJournaled(endpoint);
        });
    }

    protected abstract void journal(Command command, List<Event> events);
    public abstract long size(Class<? extends Identifiable> klass);

    public abstract Optional<Event> findEvent(UUID uuid);
    public abstract Optional<Command> findCommand(UUID uuid);

    public abstract Iterator<Event> eventIterator(Class<? extends Event> klass);
    public abstract Iterator<Command> commandIterator(Class<? extends Command> klass);
}
