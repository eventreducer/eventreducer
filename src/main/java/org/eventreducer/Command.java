package org.eventreducer;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.experimental.Accessors;
import org.apache.commons.net.ntp.TimeStamp;

import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

/**
 * Command is a request for changes in the domain. Unlike an event,
 * it is not a statement of fact as it might be rejected.
 *
 * For example, ConfirmOrder command may or may not result in an
 * OrderConfirmed event being produced.
 */
@Accessors(fluent = true)
public abstract class Command<T> extends Serializable implements Identifiable {

    public String trace = "null";

    @SneakyThrows
    public Object trace() {
        return new ObjectMapper().readValue(trace, Object.class);
    }

    @SneakyThrows
    public Command<T> trace(Object o) {
        trace = new ObjectMapper().writeValueAsString(o);
        return this;
    }

    @Setter @Accessors(fluent = true)
    private UUID uuid;

    public UUID uuid() {
        if (uuid == null) {
            uuid = UUID.randomUUID();
        }
        return uuid;
    }

    @Getter @Setter @Accessors(fluent = true)
    private TimeStamp timestamp;

    /**
     * Returns a stream of events that should be recorded
     *
     * @param endpoint Configured data access endpoint
     * @return stream of events
     * @throws Exception if the command is to be rejected, an exception has to be thrown. In this case, no events will
     *         be recorded
     */
    public abstract Stream<Event> events(Endpoint endpoint) throws Exception;

    /**
     * Once all events are recorded, this callback will be invoked with a list of
     * events.
     *
     * By default, it does nothing is meant to be overridden when necessary. For example,
     * if upon the successful recording of events an email has to be sent, this is the place
     * to do it.
     *
     * An important feature is the ability to return an optional "response" object to
     * the requestor. For example, a CreateUser command might return an <code>Optional&lt;User&gt;</code>
     * to represent the created user.
     *
     * @param endpoint Configured data access point
     * @param events list of event envelops
     * @return Optional "response" representation (any object)
     */
    public Optional<T> onCommandCompletion(Endpoint endpoint, long events) {
        return Optional.empty();
    }

    <C extends Command<T>> PublisherService<T, C> createPublisher(boolean multiple) {
        return multiple ? new MultiplePublisherService<>((C) this) : new SinglePublisherService<>((C) this);
    }
}
