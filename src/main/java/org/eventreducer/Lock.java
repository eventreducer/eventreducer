package org.eventreducer;

public interface Lock {

    void unlock();
    boolean isLocked();
}
