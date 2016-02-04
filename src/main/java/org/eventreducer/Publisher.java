package org.eventreducer;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public interface Publisher<T, C extends Command<T>> {

    void publish(C command, BiConsumer<Optional<T>, Long> completionHandler, Consumer<Throwable> exceptionHandler);

    default void publish(C command, BiConsumer<Optional<T>, Long> completionHandler) {
        publish(command, completionHandler, throwable -> {});
    }

    default void publish(C command, Consumer<Throwable> exceptionHandler) {
        publish(command,  (optional, events) -> {}, exceptionHandler);
    }

    default CompletableFuture<CommandPublished<T>> publish(C command) {
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
