package org.eventreducer;

import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.ServiceManager;
import lombok.*;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.reflections.Reflections;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Endpoint is the main interface for all eventreducer functionality
 */
@Accessors(fluent = true)
@Slf4j
public class Endpoint extends AbstractService {

    /**
     *  Package tree in which commands and events are searched for. All packages will be scanned
     *  if this parameter is omitted (can take a few seconds or more).
     */
    @Getter
    private String packagePrefix;
    /**
     * Journal implementation
     */
    @Getter
    private Journal journal;
    /**
     * Index factory implementation
     */
    @Getter
    private IndexFactory indexFactory;
    /**
     * Lock factory implementation
     */
    @Getter
    private LockFactory lockFactory;

    /**
     * If true, multiple ({@link ForkJoinPool#getCommonPoolParallelism()}) instances of
     *                           SinglePublisherService will be started for each command, and incoming commands will be
     *                           distributed amongst instances using consistent hashing of their UUIDs. <code>false</code> by default.
     */
    private boolean multiplePublishers = false;

    /**
     * @param packagePrefix Package tree in which commands and events are searched for. All packages will be scanned
     *                      if this parameter is omitted (can take a few seconds or more).
     * @param journal Journal implementation
     * @param indexFactory Index factory implementation
     * @param lockFactory Lock factory implementation
     * @param multiplePublishers If true, multiple ({@link ForkJoinPool#getCommonPoolParallelism()}) instances of
     *                           SinglePublisherService will be started for each command, and incoming commands will be
     *                           distributed amongst instances using consistent hashing of their UUIDs. <code>false</code> by default.
     */
    @Builder
    private Endpoint(String packagePrefix, @NonNull Journal journal, @NonNull IndexFactory indexFactory, @NonNull LockFactory lockFactory, boolean multiplePublishers) {
        this.packagePrefix = packagePrefix;
        this.journal = journal;
        this.indexFactory = indexFactory;
        this.lockFactory = lockFactory;
        this.multiplePublishers = multiplePublishers;
    }

    private Map<Class<? extends Command>, PublisherService<?, ?>> publisherServices = new HashMap<>();
    private ServiceManager serviceManager;


    /**
     * Main command publishing interface
     * @param command Command to be published
     * @param <T> Command return type
     * @param <C> Command type
     * @return A completable future containing command's return value and a number of events created
     */
    public <T, C extends Command<T>> CompletableFuture<Publisher.CommandPublished<T>> publish(C command) {
        return publisher((Class<Command<T>>)command.getClass()).publish(command);
    }

    /**
     * Secondary publishing interface, exposes full {@link Publisher} interface
     * @param klass Command class
     * @param <T> Command return type
     * @param <C> Command type
     * @return A corresponding {@link Publisher} interfsce
     */
    public <T, C extends Command<T>> Publisher<T, C> publisher(Class<C> klass) {
        return (Publisher<T, C>) publisherServices.get(klass);
    }

    @Override
    protected void doStart() {
        journal.endpoint(this);
        indexFactory.setJournal(journal);
        initializeIndices();
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

    private void initializeIndices() {
        Set<Class<? extends Serializer>> serializers = getSerializers();
        long e0 = System.nanoTime();
        serializers.parallelStream().forEach(t -> {
            try {
                Serializer s = t.newInstance();
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
    }

    @Override
    protected void doStop() {
        serviceManager.stopAsync().awaitStopped();
        notifyStopped();
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
