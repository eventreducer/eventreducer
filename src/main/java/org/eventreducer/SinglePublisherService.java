package org.eventreducer;

import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.ExceptionHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.EventHandlerGroup;
import com.lmax.disruptor.util.DaemonThreadFactory;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.javatuples.Triplet;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadFactory;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Stream;

@Slf4j
public class SinglePublisherService<T, C extends Command<T>> extends AbstractService implements PublisherService<T, C> {

    private final Class<? extends Command> commandClass;
    @Setter
    private Endpoint endpoint;

    /**
     * CommandEvent encapsulates published Command along with the processing state
     * sufficient for handlers/processors to complete the processing.
     */
    @NoArgsConstructor
    @Accessors(fluent = true)
    private class CommandEvent {
        /**
         * Originally supplied command
         */
        @Getter @Setter
        private C command;

        /**
         * Completion handler, supplied during publishing
         */
        @Getter @Setter
        private BiConsumer<Optional<T>, Long> completionHandler;
        /**
         * Exception handler, supplied during publishing
         */
        @Getter @Setter
        private Consumer<Throwable> exceptionHandler;
        /**
         * EventEnvelopes extracted from command events
         */
        @Getter @Setter
        private Stream<Event> events;

        @Getter @Setter
        private long eventsJournalled = -1;
    }

    public static final int RING_BUFFER_SIZE = 1024;
    private RingBuffer<CommandEvent> ringBuffer;
    private Disruptor<CommandEvent> disruptor;

    private void extractEvents(CommandEvent event, long sequence, boolean endOfBatch) throws Exception {
        try {
            event.events(event.command().events(endpoint));
        } catch (Exception e) {
            throw new EventExtractionException(e, event.command());
        }
    }

    private void journal(CommandEvent event, long sequence, boolean endOfBatch) throws Exception {
        try {
            event.eventsJournalled(endpoint.journal().save(event.command(), event.events()));
        } catch (Exception e) {
            throw new EventJournallingException(e, event.command());
        }
    }

    private void index(CommandEvent event, long sequence, boolean endOfBatch) throws Exception {
        if (event.eventsJournalled() != -1) {
            endpoint.journal().events(event.command()).parallel().forEach(e -> {
                try {
                    e.entitySerializer().index(endpoint.indexFactory(), e);
                } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e1) {
                    log.error("Error while indexing", e);
                }
            });
            event.command().entitySerializer().index(endpoint.indexFactory(), event.command());
        }

    }


    private void complete(CommandEvent event, long sequence, boolean endOfBatch) {
        if (event.events() != null) {
            event.completionHandler().accept(event.command().onCommandCompletion(endpoint, event.eventsJournalled()), event.eventsJournalled());
        }
    }


    private void translate(CommandEvent event, long sequence, Triplet<C, BiConsumer<Optional<T>, Long>, Consumer<Throwable>> message) {
        event.
            command(message.getValue0()).
            completionHandler(((optional, events) -> message.getValue1().accept(optional, events))).
            exceptionHandler(message.getValue2());
    }

    /**
     * Publishes a Command to the disruptor
     * @param command Command to be published
     * @param completionHandler Completion handler to be used once the command has been successfully processed
     * @param exceptionHandler Exception handler to be used if an exception get thrown while processing the command
     */
    @Override
    public void publish(C command, BiConsumer<Optional<T>, Long> completionHandler, Consumer<Throwable> exceptionHandler) {
        ringBuffer.publishEvent(this::translate, Triplet.with(command, completionHandler, exceptionHandler));
    }

    public SinglePublisherService(C command) {
        commandClass = command.getClass();
    }

    @Override
    @SneakyThrows
    protected void doStart() {
        log.debug("Starting single publisher {}", commandClass.getSimpleName());

        ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("eventreducer-" + commandClass.getSimpleName() +"-%d").setDaemon(true).build();

        disruptor = new Disruptor<>(CommandEvent::new, RING_BUFFER_SIZE, threadFactory);
        disruptor.setDefaultExceptionHandler(new CommandEventExceptionHandler());

        List<EventHandler<CommandEvent>> eventHandlers =
                Arrays.asList(this::extractEvents, this::journal, this::index, this::complete);

        EventHandlerGroup<CommandEvent> handler = disruptor.handleEventsWith(eventHandlers.get(0));

        for (EventHandler<CommandEvent> h : eventHandlers.subList(1, eventHandlers.size())) {
            handler = handler.then(h);
        }

        ringBuffer = disruptor.start();

        notifyStarted();
    }

    @Override
    protected void doStop() {
        disruptor.shutdown();
        notifyStopped();
    }

    private class CommandEventExceptionHandler implements ExceptionHandler<CommandEvent> {
        @Override
        public void handleEventException(Throwable ex, long sequence, CommandEvent event) {
            event.exceptionHandler().accept(ex);
        }

        @Override
        public void handleOnStartException(Throwable ex) {

        }

        @Override
        public void handleOnShutdownException(Throwable ex) {

        }
    }
}
