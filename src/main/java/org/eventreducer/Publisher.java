package org.eventreducer;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
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

    default CompletableFuture<CommandPublished> publish(Command command) {
        CompletableFuture<CommandPublished> future = new CompletableFuture<>();
        publish(command, (optional, events) ->
                future.complete(new CommandPublished(optional, events)), future::completeExceptionally);
        return future;
    }

    @AllArgsConstructor
    class CommandPublished {
        @Getter
        private Optional result;
        @Getter
        private List<Event> events;
    }
}
