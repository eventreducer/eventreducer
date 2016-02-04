package org.eventreducer;

import com.google.common.util.concurrent.Service;

public interface PublisherService<T, C extends Command<T>> extends Publisher<T, C>, Service {

    void setEndpoint(Endpoint endpoint);
}
