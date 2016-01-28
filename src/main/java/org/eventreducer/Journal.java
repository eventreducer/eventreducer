package org.eventreducer;

import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.net.ntp.TimeStamp;
import org.eventreducer.hlc.HybridTimestamp;
import org.eventreducer.hlc.PhysicalTimeProvider;

import java.util.Iterator;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

public abstract class Journal implements EndpointComponent {


    @Getter @Setter @Accessors(fluent = true)
    private Endpoint endpoint;
    private HybridTimestamp timestamp;

    public Journal(PhysicalTimeProvider physicalTimeProvider) {
        this.timestamp = new HybridTimestamp(physicalTimeProvider);
    }

    public long save(Command command, Stream<Event> events) throws Exception {
        TimeStamp commandTimestamp = new TimeStamp(timestamp.update());
        command.timestamp(commandTimestamp);

        if (!(command instanceof EphemeralCommand)) {
            return  journal(command, events.map(event -> event.command(command).timestamp(new TimeStamp(timestamp.update()))));
        }

        return 0;
    }

    protected abstract long journal(Command command, Stream<Event> events);
    public abstract long size(Class<? extends Identifiable> klass);

    public abstract Optional<Event> findEvent(UUID uuid);
    public abstract Optional<Command> findCommand(UUID uuid);

    public abstract Iterator<Event> eventIterator(Class<? extends Event> klass);
    public abstract Iterator<Command> commandIterator(Class<? extends Command> klass);

    public abstract Stream<Event> events(Command command);
}
