package org.eventreducer;

import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.net.ntp.TimeStamp;
import org.eventreducer.hlc.HybridTimestamp;
import org.eventreducer.hlc.PhysicalTimeProvider;

import java.util.List;

public abstract class Journal implements EndpointComponent {


    @Getter @Setter @Accessors(fluent = true)
    private Endpoint endpoint;
    private HybridTimestamp timestamp;

    public Journal(PhysicalTimeProvider physicalTimeProvider) {
        this.timestamp = new HybridTimestamp(physicalTimeProvider);
    }

    public void save(Command command, List<Event> events) throws Exception {
        events.stream().
                forEachOrdered(event -> {
                    event.
                            command(command).
                            timestamp(new TimeStamp(timestamp.update()));
                });
        journal(events);
        events.stream().forEachOrdered(event -> {
            event.onEventJournaled(endpoint);
        });
    }

    protected abstract void journal(List<Event> events);
    public abstract long size();

    public void prepareIndices(IndexFactory indexFactory) {}
}
