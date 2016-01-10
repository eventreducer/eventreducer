package org.eventreducer;

import java.util.concurrent.Semaphore;

public class MemoryLockFactory extends LockFactory {

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
