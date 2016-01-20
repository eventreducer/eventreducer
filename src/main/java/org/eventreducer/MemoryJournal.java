package org.eventreducer;

import org.eventreducer.hlc.PhysicalTimeProvider;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class MemoryJournal extends Journal {

    protected Map<UUID, Event> storage = new HashMap<>();
    protected Map<UUID, Command> commands = new HashMap<>();

    public MemoryJournal(PhysicalTimeProvider physicalTimeProvider) {
        super(physicalTimeProvider);
    }

    @Override
    protected void journal(Command command, List<Event> events) {
        commands.put(command.uuid(), command);
        events.stream().
                forEachOrdered(event -> storage.put(event.uuid(), event));
    }

    @Override
    public long size() {
        return storage.size();
    }

}
