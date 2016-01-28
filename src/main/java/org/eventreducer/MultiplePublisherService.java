package org.eventreducer;

import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import com.google.common.primitives.Bytes;
import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.ServiceManager;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ForkJoinPool;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

@Slf4j
public class MultiplePublisherService extends AbstractService implements PublisherService {

    private PublisherService[] publishers;
    private ServiceManager serviceManager;

    public MultiplePublisherService(int nPublishers) {
        publishers = new PublisherService[nPublishers];
        for (int i = 0; i < nPublishers; i++) {
            publishers[i] = new SinglePublisherService();
        }
    }

    public MultiplePublisherService() {
        this(ForkJoinPool.getCommonPoolParallelism());
    }

    @Override
    protected void doStart() {
        log.info("Starting multiple publisher ({} instances)", publishers.length);
        serviceManager = new ServiceManager(Arrays.asList(publishers));
        serviceManager.startAsync().awaitHealthy();
        notifyStarted();
    }

    @Override
    protected void doStop() {
        serviceManager.stopAsync().awaitStopped();
        notifyStopped();
    }

    @Override
    public <T> void publish(Command<T> command, BiConsumer<Optional<T>, Long> completionHandler, Consumer<Throwable> exceptionHandler) {
        UUID uuid = command.uuid();
        HashCode hashCode = HashCode.fromBytes(Bytes.concat(Longs.toByteArray(uuid.getMostSignificantBits()), Longs.toByteArray(uuid.getLeastSignificantBits())));
        int bucket = Hashing.consistentHash(hashCode, publishers.length);
        publishers[bucket].publish(command, completionHandler, exceptionHandler);
    }

    @Override
    public void setEndpoint(Endpoint endpoint) {
        for (PublisherService publisher : publishers) {
            publisher.setEndpoint(endpoint);
        }
    }
}
