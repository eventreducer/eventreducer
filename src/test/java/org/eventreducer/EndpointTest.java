package org.eventreducer;

import lombok.SneakyThrows;
import org.eventreducer.annotations.*;
import org.eventreducer.hlc.NTPServerTimeProvider;
import org.junit.Test;

import java.util.List;
import java.util.stream.Stream;

public class EndpointTest {

    @org.eventreducer.annotations.Serializable
    public static class TestCommand extends Command<Void> {
        @Override
        public Stream<Event> events(Endpoint endpoint) throws Exception {
            throw new Exception("exception");
        }
    }

    @Test
    @SneakyThrows
    public void test()  {
        NTPServerTimeProvider timeProvider = new NTPServerTimeProvider();
        timeProvider.startAsync().awaitRunning();
        MemoryJournal journal = new MemoryJournal(timeProvider);
        Endpoint endpoint = Endpoint.builder().journal(journal).build();
        endpoint.startAsync().awaitRunning();

        Publisher publisher = endpoint.publisher(TestCommand.class);
        publisher.publish(new TestCommand(), (optional, events) -> {
            System.out.println(optional);
        }, System.out::println);

        endpoint.stopAsync().awaitTerminated();

    }
}