package org.eventreducer;

import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.ServiceManager;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.reflections.Reflections;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

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

    private Map<Class<? extends Command>, PublisherService<?, ?>> publisherServices = new HashMap<>();
    private boolean multiplePublishers;
    private ServiceManager serviceManager;

    public Endpoint() {
        this(false);
    }

    public Endpoint(boolean multiplePublishers) {
        this.multiplePublishers = multiplePublishers;
    }

    public Endpoint(boolean multiple, String packagePrefix) {
        this(multiple);
        this.packagePrefix = packagePrefix;
    }

    public Endpoint(String packagePrefix) {
        this();
        this.packagePrefix = packagePrefix;
    }

    public <T, C extends Command<T>> CompletableFuture<Publisher.CommandPublished<T>> publish(C command) {
        return publisher((Class<Command<T>>)command.getClass()).publish(command);
    }

    public <T, C extends Command<T>> Publisher<T, C> publisher(Class<C> klass) {
        return (Publisher<T, C>) publisherServices.get(klass);
    }

    @Override
    protected void doStart() {
        getCommands().forEach(new Consumer<Class<? extends Command>>() {
            @Override @SneakyThrows
            public void accept(Class<? extends Command> klass) {
                PublisherService publisher = klass.newInstance().createPublisher(multiplePublishers);
                publisher.setEndpoint(Endpoint.this);
                publisherServices.put(klass, publisher);
                assert publisherServices.containsKey(klass);
            }
        });
        serviceManager = new ServiceManager(publisherServices.values());
        serviceManager.startAsync().awaitHealthy();
        notifyStarted();
    }

    @Override
    protected void doStop() {
        serviceManager.stopAsync().awaitStopped();
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
        Set<Class<? extends Serializer>> serializers = getSerializers();
        long e0 = System.nanoTime();
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
        long e1 = System.nanoTime();
        log.info("Index preparation is done, total time elapsed: {} seconds.",
                TimeUnit.SECONDS.convert(e1-e0, TimeUnit.NANOSECONDS));
        return this;
    }

    private Set<Class<? extends Serializer>> getSerializers() {
        Reflections reflections = packagePrefix == null ? new Reflections() : new Reflections(packagePrefix);
        return reflections.getSubTypesOf(Serializer.class);
    }

    private Set<Class<? extends Command>> getCommands() {
        return getSerializers().stream().map(s -> s.getAnnotation(org.eventreducer.annotations.Serializer.class).
                value()).filter(Command.class::isAssignableFrom).map(c -> (Class<? extends Command>)c).collect(Collectors.toSet());
    }


}
