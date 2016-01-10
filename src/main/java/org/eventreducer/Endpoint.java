package org.eventreducer;

import com.google.common.util.concurrent.AbstractService;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.eventreducer.annotations.Serializer;
import org.reflections.Reflections;

import java.util.Set;

@Accessors(fluent = true)
@Slf4j
public class Endpoint extends AbstractService {

    @Getter
    private Journal journal;
    @Getter
    private IndexFactory indexFactory;
    @Getter @Setter
    private LockFactory lockFactory;

    private CommandDisruptor commandDisruptor;


    public Endpoint() {
        commandDisruptor = new CommandDisruptor(this);
    }

    public Publisher publisher() {
        return commandDisruptor;
    }

    @Override
    protected void doStart() {
        journal().prepareIndices(indexFactory());
        commandDisruptor.startAsync().awaitRunning();
        notifyStarted();
    }

    @Override
    protected void doStop() {
        commandDisruptor.stopAsync().awaitTerminated();
        notifyStopped();
    }

    public Endpoint journal(Journal journal) {
        this.journal = journal;
        journal.endpoint(this);
        return this;
    }

    public Endpoint indexFactory(IndexFactory indexFactory) {
        this.indexFactory = indexFactory;
        Set<Class<?>> serializers = new Reflections().getTypesAnnotatedWith(Serializer.class);
        serializers.parallelStream().forEach(t -> {
            try {
                org.eventreducer.Serializer s = (org.eventreducer.Serializer) t.newInstance();
                s.configureIndices(indexFactory);
            } catch (InstantiationException | IllegalAccessException e) {
                log.error("Error while initializing index factory", e);
            } catch (IndexFactory.IndexNotSupported indexNotSupported) {
                log.error("Error while initializing index factory", indexNotSupported);
            }
        });
        return this;
    }


}
