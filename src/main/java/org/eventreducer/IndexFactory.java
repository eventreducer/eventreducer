package org.eventreducer;

import com.googlecode.cqengine.IndexedCollection;
import com.googlecode.cqengine.attribute.Attribute;
import com.googlecode.cqengine.index.Index;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.javatuples.Triplet;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

public abstract class IndexFactory implements EndpointComponent {

    @Getter @Setter
    private Journal journal;

    @Getter
    @Setter
    @Accessors(fluent = true)
    private Endpoint endpoint;

    public enum IndexFeature {
        UNIQUE, COMPOUND,

        EQ,
        IN,
        LT,
        GT,
        BT,
        SW,
        EW,
        SC,
        CI,
        RX,
        HS,
        AQ,
        QZ
    }

    protected abstract List<Triplet<String, IndexFeature[], Function<Attribute, Index>>> getIndexMatrix();

    public <A, O> Index<A> getIndexOnAttribute(Attribute<A, O> attribute, IndexFeature...features) throws IndexNotSupported {
        for (Triplet<String, IndexFeature[], Function<Attribute, Index>> triplet : getIndexMatrix()) {
            if (Arrays.asList(triplet.getValue1()).containsAll(Arrays.asList(features))) {
                return triplet.getValue2().apply(attribute);
            }
        }
        throw new IndexNotSupported();
    }

    public static class IndexNotSupported extends Exception {
    }

    public abstract <O> IndexedCollection<O> getIndexedCollection(Class<O> klass);
}
