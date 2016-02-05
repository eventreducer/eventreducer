package org.eventreducer.annotations;

import com.googlecode.cqengine.attribute.SimpleAttribute;
import com.googlecode.cqengine.query.option.QueryOptions;
import com.googlecode.cqengine.resultset.ResultSet;
import lombok.SneakyThrows;
import org.eventreducer.Endpoint;
import org.eventreducer.Event;
import org.eventreducer.MemoryIndexFactory;
import org.eventreducer.MemoryJournal;
import org.eventreducer.hlc.NTPServerTimeProvider;
import org.junit.Test;

import static com.googlecode.cqengine.query.QueryFactory.*;
import static org.junit.Assert.assertEquals;

public class AnnotationProcessorTest {

    public static class TestEvent extends Event {
        @Property
        public String str;
        @Property
        public int i;
        @Property
        public int[] c;

        @Index
        public static final SimpleAttribute<TestEvent, String> STR = new SimpleAttribute<TestEvent, String>() {
            @Override
            public String getValue(TestEvent object, QueryOptions queryOptions) {
                return object.str;
            }
        };
    }

    @Test
    @SneakyThrows
    public void testIndexing() {
        Endpoint endpoint = new Endpoint().journal(new MemoryJournal(new NTPServerTimeProvider())).indexFactory(new MemoryIndexFactory());
        endpoint.startAsync().awaitRunning();

        TestEvent testEvent = new TestEvent();
        testEvent.str = "test";

        org.eventreducer.Serializer<TestEvent> serializer = testEvent.entitySerializer();

        serializer.index(endpoint.indexFactory(), testEvent);
        ResultSet<TestEvent> resultSet = serializer.getIndex(endpoint.indexFactory()).retrieve(equal(TestEvent.STR, "test"));
        assertEquals(1, resultSet.size());
        assertEquals(testEvent, resultSet.uniqueResult());

        resultSet.close();
    }

}