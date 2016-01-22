package org.eventreducer;

import org.eventreducer.hlc.PhysicalTimeProvider;

import java.util.*;

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
        return storage.size() + commands.size();
    }

    @Override
    public Optional<Event> findEvent(UUID uuid) {
        if (storage.containsKey(uuid)) {
            return Optional.of(storage.get(uuid));
        }
        return Optional.empty();
    }

    @Override
    public Optional<Command> findCommand(UUID uuid) {
        if (commands.containsKey(uuid)) {
            return Optional.of(commands.get(uuid));
        }
        return Optional.empty();
    }

    @Override
    public Iterator<Event> eventIterator(Class<? extends Event> klass) {
        return storage.values().stream().filter(v -> klass.isAssignableFrom(v.getClass())).iterator();
    }

    @Override
    public Iterator<Command> commandIterator(Class<? extends Command> klass) {
        return commands.values().stream().filter(v -> klass.isAssignableFrom(v.getClass())).iterator();
    }


}
