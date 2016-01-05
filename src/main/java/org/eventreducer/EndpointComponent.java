package org.eventreducer;

public interface EndpointComponent {

    EndpointComponent endpoint(Endpoint endpoint);
    Endpoint endpoint();
}
