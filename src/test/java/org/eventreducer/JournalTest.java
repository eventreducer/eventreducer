package org.eventreducer;

import lombok.SneakyThrows;
import org.eventreducer.hlc.NTPServerTimeProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class JournalTest {

    @Parameterized.Parameters(name = "{index}: {0}")
    @SneakyThrows
    public static Collection<Journal> journals() {
        NTPServerTimeProvider physicalTimeProvider = new NTPServerTimeProvider();
        physicalTimeProvider.startAsync().awaitRunning();

        MemoryJournal memoryJournal = new MemoryJournal(physicalTimeProvider);
        Endpoint.builder().journal(memoryJournal).build();

        return Arrays.asList(memoryJournal);
    }

    @Parameterized.Parameter
    public Journal journal;

    @Before
    public void setup() {
        journal.endpoint().startAsync().awaitRunning();
    }

    @After
    public void teardown() {
        journal.endpoint().stopAsync().awaitTerminated();
    }

    private static class TestCommand extends Command {
        @Override
        public Stream<Event> events(Endpoint endpoint) throws Exception {
            return Stream.of(new TestEvent());
        }
    }

    private static class TestEvent extends Event {
    }

    @Test
    @SneakyThrows
    public void test() {
        TestCommand command = new TestCommand();
        assertEquals(0, journal.size(TestCommand.class));
        journal.save(command, command.events(journal.endpoint()));
        assertEquals(1, journal.size(TestCommand.class));
    }

}