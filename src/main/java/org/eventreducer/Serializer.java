package org.eventreducer;

import com.googlecode.cqengine.IndexedCollection;

public abstract class Serializer<T extends Serializable> {
    public abstract byte[] hash();

    public abstract void configureIndices(IndexFactory indexFactory) throws IndexFactory.IndexNotSupported;
    public abstract void index(IndexFactory indexFactory, T o);
    public abstract IndexedCollection<T> getIndex(IndexFactory indexFactory);

    public abstract String toString(T serializable);
}
