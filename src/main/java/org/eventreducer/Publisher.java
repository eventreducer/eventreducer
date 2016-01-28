package org.eventreducer;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public interface Publisher {

    <T> void publish(Command<T> command, BiConsumer<Optional<T>, Long> completionHandler, Consumer<Throwable> exceptionHandler);

    default <T> void publish(Command<T> command, BiConsumer<Optional<T>, Long> completionHandler) {
        publish(command, completionHandler, throwable -> {});
    }

    default void publish(Command command, Consumer<Throwable> exceptionHandler) {
        publish(command,  (optional, events) -> {}, exceptionHandler);
    }

    default <T> CompletableFuture<CommandPublished<T>> publish(Command<T> command) {
        CompletableFuture<CommandPublished<T>> future = new CompletableFuture<>();
        publish(command, (Optional<T> optional, Long events) ->
                future.complete(new CommandPublished<>(optional, events)), future::completeExceptionally);
        return future;
    }

    @AllArgsConstructor
    class CommandPublished<T> {
        @Getter
        private Optional<T> result;
        @Getter
        private long events;
    }
}
