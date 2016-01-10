package org.eventreducer;

import org.eventreducer.hlc.PhysicalTimeProvider;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class MemoryJournal extends Journal {

    private Map<UUID, Event> storage = new HashMap<>();

    public MemoryJournal(PhysicalTimeProvider physicalTimeProvider) {
        super(physicalTimeProvider);
    }

    @Override
    protected void journal(List<Event> events) {
        events.stream().
                forEachOrdered(event -> storage.put(event.uuid(), event));
    }

    @Override
    public long size() {
        return storage.size();
    }

}
