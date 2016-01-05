package org.eventreducer;

import java.util.List;
import java.util.Optional;

/**
 * Command is a request for changes in the domain. Unlike an event,
 * it is not a statement of fact as it might be rejected.
 *
 * For example, ConfirmOrder command may or may not result in an
 * OrderConfirmed event being produced.
 */
public abstract class Command extends Serializable {
    /**
     * Returns a list of events that should be recorded
     *
     * @param endpoint Configured data access endpoint
     * @return list of events
     * @throws Exception if the command is to be rejected, an exception has to be thrown. In this case, no events will
     *         be recorded
     */
    public abstract List<Event> events(Endpoint endpoint) throws Exception;

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
    public Optional onCommandCompletion(Endpoint endpoint, List<Event> events) {
        return Optional.empty();
    }
}
