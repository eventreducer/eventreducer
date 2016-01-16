package org.eventreducer;

import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public interface Publisher {

    void publish(Command command, BiConsumer<Optional, List<Event>> completionHandler, Consumer<Throwable> exceptionHandler);

    default void publish(Command command, BiConsumer<Optional, List<Event>> completionHandler) {
        publish(command, completionHandler, throwable -> {});
    }

    default void publish(Command command, Consumer<Throwable> exceptionHandler) {
        publish(command,  (optional, events) -> {}, exceptionHandler);
    }

    default void publish(Command command) {
        publish(command, (optional, events) -> {});
    }

}
