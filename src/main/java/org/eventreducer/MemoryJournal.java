package org.eventreducer;

import org.eventreducer.hlc.PhysicalTimeProvider;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Semaphore;

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

    @Override
    public org.eventreducer.Lock lock(Object lock) {
        Semaphore semaphore = new Semaphore(1);
        semaphore.acquireUninterruptibly();
        return new MemoryLock(semaphore);
    }

    static class MemoryLock implements Lock {
        private final Semaphore semaphore;

        public MemoryLock(Semaphore semaphore) {
            this.semaphore = semaphore;
        }

        @Override
        public void unlock() {
            semaphore.release();
        }

    }
}
