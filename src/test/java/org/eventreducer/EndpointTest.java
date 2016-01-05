package org.eventreducer;

import lombok.SneakyThrows;
import org.eventreducer.hlc.NTPServerTimeProvider;
import org.junit.Test;

import java.util.List;

public class EndpointTest {

    private static class TestCommand extends Command {
        @Override
        public List<Event> events(Endpoint endpoint) throws Exception {
            throw new Exception("exception");
        }
    }

    @Test
    @SneakyThrows
    public void test()  {
        NTPServerTimeProvider timeProvider = new NTPServerTimeProvider();
        timeProvider.startAsync().awaitRunning();
        MemoryJournal journal = new MemoryJournal(timeProvider);
        Endpoint endpoint = new Endpoint().journal(journal);
        endpoint.startAsync().awaitRunning();

        Publisher publisher = endpoint.publisher();
        publisher.publish(new TestCommand(), (optional, events) -> {
            System.out.println(optional);
        }, System.out::println);

        endpoint.stopAsync().awaitTerminated();

    }
}