package org.eventreducer;

import com.google.common.util.concurrent.AbstractService;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.reflections.Reflections;

import java.util.Set;
import java.util.concurrent.TimeUnit;

@Accessors(fluent = true)
@Slf4j
public class Endpoint extends AbstractService {

    private String packagePrefix;
    @Getter
    private Journal journal;
    @Getter
    private IndexFactory indexFactory;
    @Getter @Setter
    private LockFactory lockFactory;

    private PublisherService publisherService;


    public Endpoint() {
        this(new SinglePublisherService());
    }

    public Endpoint(PublisherService publisherService) {
        this.publisherService = publisherService;
        this.publisherService.setEndpoint(this);
    }

    public Endpoint(PublisherService publisherService, String packagePrefix) {
        this(publisherService);
        this.packagePrefix = packagePrefix;
    }

    public Endpoint(String packagePrefix) {
        this();
        this.packagePrefix = packagePrefix;
    }

    public Publisher publisher() {
        return publisherService;
    }

    @Override
    protected void doStart() {
        publisherService.startAsync().awaitRunning();
        notifyStarted();
    }

    @Override
    protected void doStop() {
        publisherService.stopAsync().awaitTerminated();
        notifyStopped();
    }

    public Endpoint journal(Journal journal) {
        this.journal = journal;
        journal.endpoint(this);
        return this;
    }

    public Endpoint indexFactory(IndexFactory indexFactory) {
        indexFactory.setJournal(journal());
        this.indexFactory = indexFactory;
        Reflections reflections = packagePrefix == null ? new Reflections() : new Reflections(packagePrefix);
        Set<Class<? extends org.eventreducer.Serializer>> serializers = reflections.getSubTypesOf(org.eventreducer.Serializer.class);
        serializers.parallelStream().forEach(t -> {
            try {
                org.eventreducer.Serializer s = t.newInstance();
                Class serializable = s.getClass().getAnnotation(org.eventreducer.annotations.Serializer.class).value();
                log.info("{}: Configuring indices", serializable.getSimpleName());
                long t0 = System.nanoTime();
                s.configureIndices(indexFactory);
                long t1 = System.nanoTime();
                log.info("{}: Done configuring indices, elapsed time {} seconds, size: {}.",
                        serializable.getSimpleName(),
                        TimeUnit.SECONDS.convert(t1-t0, TimeUnit.NANOSECONDS), s.getIndex(indexFactory).size());

            } catch (InstantiationException | IllegalAccessException e) {
                log.error("Error while initializing index factory", e);
            } catch (IndexFactory.IndexNotSupported indexNotSupported) {
                log.error("Error while initializing index factory", indexNotSupported);
            }
        });
        return this;
    }


}
