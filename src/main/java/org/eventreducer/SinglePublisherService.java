package org.eventreducer;

import com.google.common.util.concurrent.AbstractService;
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
import java.util.function.BiConsumer;
import java.util.function.Consumer;

@Slf4j
public class SinglePublisherService extends AbstractService implements PublisherService {

    @Setter
    private Endpoint endpoint;

    /**
     * CommandEvent encapsulates published Command along with the processing state
     * sufficient for handlers/processors to complete the processing.
     */
    @NoArgsConstructor
    @Accessors(fluent = true)
    private static class CommandEvent {
        /**
         * Originally supplied command
         */
        @Getter @Setter
        private Command command;

        /**
         * Completion handler, supplied during publishing
         */
        @Getter @Setter
        private BiConsumer<Optional, List<Event>> completionHandler;
        /**
         * Exception handler, supplied during publishing
         */
        @Getter @Setter
        private Consumer<Throwable> exceptionHandler;
        /**
         * EventEnvelopes extracted from command events
         */
        @Getter @Setter
        private List<Event> events;
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
            if (event.events() != null) {
                endpoint.journal().save(event.command(), event.events());
            }
        } catch (Exception e) {
            throw new EventExtractionException(e, event.command());
        }
    }

    private void index(CommandEvent event, long sequence, boolean endOfBatch) throws Exception {
        if (event.events != null) {
            event.command().entitySerializer().index(endpoint.indexFactory(), event.command());
            event.events().parallelStream().forEach(e -> {
                try {
                    e.entitySerializer().index(endpoint.indexFactory(), e);
                } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e1) {
                    log.error("Error while indexing", e);
                }
            });
        }

    }


    private void complete(CommandEvent event, long sequence, boolean endOfBatch) {
        if (event.events() != null) {
            event.completionHandler().accept(event.command().onCommandCompletion(endpoint, event.events()), event.events());
        }
    }


    private <T> void translate(CommandEvent event, long sequence, Triplet<Command<T>, BiConsumer<Optional<T>, List<Event>>, Consumer<Throwable>> message) {
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
    public <T> void publish(Command<T> command, BiConsumer<Optional<T>, List<Event>> completionHandler, Consumer<Throwable> exceptionHandler) {
        ringBuffer.publishEvent(this::translate, Triplet.with(command, completionHandler, exceptionHandler));
    }

    @Override
    @SneakyThrows
    protected void doStart() {
        log.info("Starting single publisher");

        disruptor = new Disruptor<>(CommandEvent::new, RING_BUFFER_SIZE, DaemonThreadFactory.INSTANCE);
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

    private static class CommandEventExceptionHandler implements ExceptionHandler<CommandEvent> {
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
