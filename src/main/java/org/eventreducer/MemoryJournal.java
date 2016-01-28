package org.eventreducer;

import org.eventreducer.hlc.PhysicalTimeProvider;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MemoryJournal extends Journal {

    protected Map<UUID, Event> storage = new HashMap<>();
    protected Map<UUID, Command> commands = new HashMap<>();

    public MemoryJournal(PhysicalTimeProvider physicalTimeProvider) {
        super(physicalTimeProvider);
    }

    @Override
    protected long journal(Command command, Stream<Event> events) {
        commands.put(command.uuid(), command);
        return events.map(event -> storage.put(event.uuid(), event)).count();
    }

    @Override
    public long size(Class<? extends Identifiable> klass) {
        return storage.values().stream().filter(e -> klass.isAssignableFrom(e.getClass())).collect(Collectors.toList()).size() +
               commands.values().stream().filter(e -> klass.isAssignableFrom(e.getClass())).collect(Collectors.toList()).size();
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

    @Override
    public Stream<Event> events(Command command) {
        return storage.values().stream().filter(e -> e.command().uuid().equals(command.uuid()));
    }


}
