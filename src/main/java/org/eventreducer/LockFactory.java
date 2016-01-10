package org.eventreducer;

import java.util.function.Supplier;

public abstract class LockFactory {

    public abstract Lock lock(Object lock);

    public <T>T withLock(Object lock, Supplier<T> supplier) {
        Lock l = lock(lock);
        T t = supplier.get();
        l.unlock();
        return t;
    }

}
