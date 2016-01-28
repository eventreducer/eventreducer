package org.eventreducer;

import java.util.stream.Stream;

public class EventlessCommand<T> extends Command<T> {
    @Override
    public final Stream<Event> events(Endpoint endpoint) throws Exception {
        return Stream.empty();
    }
}
