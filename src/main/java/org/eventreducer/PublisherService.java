package org.eventreducer;

import com.google.common.util.concurrent.Service;

public interface PublisherService extends Publisher, Service {

    void setEndpoint(Endpoint endpoint);
}
