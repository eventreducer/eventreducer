package org.eventreducer;

import com.googlecode.cqengine.attribute.SimpleAttribute;
import com.googlecode.cqengine.index.Index;
import com.googlecode.cqengine.persistence.Persistence;
import com.googlecode.cqengine.query.option.QueryOptions;

import java.sql.Connection;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;
import java.util.stream.StreamSupport;

public class JournalPersistence<O extends Identifiable> implements Persistence<O, UUID> {

    private final Journal journal;
    private final Class<O> klass;

    public JournalPersistence(Journal journal, Class<O> klass) {
        this.journal = journal;
        this.klass = klass;
    }

    @Override
    public SimpleAttribute<O, UUID> getPrimaryKeyAttribute() {
        return new SimpleAttribute<O, UUID>() {
            @Override
            public UUID getValue(O object, QueryOptions queryOptions) {
                return object.uuid();
            }
        };
    }

    @Override
    public long getBytesUsed() {
        return 0;
    }

    @Override
    public void compact() {

    }

    @Override
    public void expand(long numBytes) {

    }

    @Override
    public Connection getConnection(Index<?> index) {
        return null;
    }

    @Override
    public boolean isApplyUpdateForIndexEnabled(Index<?> index) {
        return false;
    }

    @Override
    public Set<O> create() {
        return new JournalSet<>(journal, klass);
    }

    static class JournalSet<O extends Identifiable> implements Set<O> {

        private final Journal journal;
        private final Class<O> klass;

        public JournalSet(Journal journal, Class<O> klass) {
            this.journal = journal;
            this.klass = klass;
        }

        @Override
        public int size() {
            return (int) journal.size(klass);
        }

        @Override
        public boolean isEmpty() {
            return size() == 0;
        }

        @Override
        public boolean contains(Object o) {
            if (Event.class.isAssignableFrom(o.getClass())) {
                return journal.findEvent(((Event)o).uuid()).isPresent();
            }
            if (Command.class.isAssignableFrom(o.getClass())) {
                return journal.findCommand(((Command)o).uuid()).isPresent();
            }
            return false;
        }

        @Override
        public Iterator<O> iterator() {
            if (Event.class.isAssignableFrom(klass)) {
                return (Iterator<O>) journal.eventIterator((Class<? extends Event>) klass);
            } else {
                return (Iterator<O>) journal.commandIterator((Class<? extends Command>) klass);
            }
        }

        @Override
        public Object[] toArray() {
            return StreamSupport.stream(spliterator(), false).toArray();
        }

        @Override
        public <T> T[] toArray(T[] a) {
            return (T[]) StreamSupport.stream(spliterator(), false).toArray();
        }

        @Override
        public boolean add(O o) {
            return true; // journalling is done separately
        }

        @Override
        public boolean remove(Object o) {
            return false; // we never remove objects
        }

        @Override
        public boolean containsAll(Collection<?> c) {
            return c.stream().allMatch(this::contains);
        }

        @Override
        public boolean addAll(Collection<? extends O> c) {
            return true; // journalling is done separately
        }

        @Override
        public boolean retainAll(Collection<?> c) {
            return false; // we never remove objects
        }

        @Override
        public boolean removeAll(Collection<?> c) {
            return false; // we never remove objects
        }

        @Override
        public void clear() {
            // we never remove objects
        }
    }
}
