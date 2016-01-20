package org.eventreducer;

import lombok.Getter;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Semaphore;

public class MemoryLockFactory extends LockFactory {

    private Map<Object, Semaphore> locks = new HashMap<>();

    @Override
    public org.eventreducer.Lock lock(Object lock) {
        Semaphore semaphore = locks.containsKey(lock) ? locks.get(lock) : new Semaphore(1);
        locks.put(lock, semaphore);
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

        @Override
        public boolean isLocked() {
            return semaphore.availablePermits() == 0;
        }

    }

}
