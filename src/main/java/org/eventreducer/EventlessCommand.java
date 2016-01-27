package org.eventreducer;

import java.util.Collections;
import java.util.List;

public class EventlessCommand<T> extends Command<T> {
    @Override
    public final List<Event> events(Endpoint endpoint) throws Exception {
        return Collections.emptyList();
    }
}
