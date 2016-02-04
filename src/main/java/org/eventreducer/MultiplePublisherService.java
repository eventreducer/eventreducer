package org.eventreducer;

import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import com.google.common.primitives.Bytes;
import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.ServiceManager;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.ForkJoinPool;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
public class MultiplePublisherService<T, C extends Command<T>> extends AbstractService implements PublisherService<T, C> {

    private Class<? extends Command> commandClass;
    private List<PublisherService<T, C>> publishers;
    private ServiceManager serviceManager;

    public MultiplePublisherService(C command) {
        commandClass = command.getClass();
        publishers = new LinkedList<>();
        for (int i = 0; i < ForkJoinPool.getCommonPoolParallelism(); i++) {
            publishers.add(new SinglePublisherService<>(command));
        }
    }


    @Override
    protected void doStart() {
        log.debug("Starting multiple publisher {} ({} instances)", commandClass.getSimpleName(), publishers.size());
        serviceManager = new ServiceManager(publishers);
        serviceManager.startAsync().awaitHealthy();
        notifyStarted();
    }

    @Override
    protected void doStop() {
        serviceManager.stopAsync().awaitStopped();
        notifyStopped();
    }

    @Override
    public void publish(C command, BiConsumer<Optional<T>, Long> completionHandler, Consumer<Throwable> exceptionHandler) {
        UUID uuid = command.uuid();
        HashCode hashCode = HashCode.fromBytes(Bytes.concat(Longs.toByteArray(uuid.getMostSignificantBits()), Longs.toByteArray(uuid.getLeastSignificantBits())));
        int bucket = Hashing.consistentHash(hashCode, publishers.size());
        publishers.get(bucket).publish(command, completionHandler, exceptionHandler);
    }

    @Override
    public void setEndpoint(Endpoint endpoint) {
        for (PublisherService publisher : publishers) {
            publisher.setEndpoint(endpoint);
        }
    }
}
