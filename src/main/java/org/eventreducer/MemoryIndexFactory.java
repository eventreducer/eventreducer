package org.eventreducer;

import com.googlecode.cqengine.ConcurrentIndexedCollection;
import com.googlecode.cqengine.IndexedCollection;
import com.googlecode.cqengine.attribute.Attribute;
import com.googlecode.cqengine.index.Index;
import com.googlecode.cqengine.index.hash.HashIndex;
import com.googlecode.cqengine.index.navigable.NavigableIndex;
import com.googlecode.cqengine.index.radix.RadixTreeIndex;
import com.googlecode.cqengine.index.radixinverted.InvertedRadixTreeIndex;
import com.googlecode.cqengine.index.radixreversed.ReversedRadixTreeIndex;
import com.googlecode.cqengine.index.suffix.SuffixTreeIndex;
import org.javatuples.Triplet;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import static org.eventreducer.IndexFactory.IndexFeature.*;

public class MemoryIndexFactory extends IndexFactory {

    private Map<String, IndexedCollection> indexedCollections = new ConcurrentHashMap<>();

    @Override
    protected List<Triplet<String, IndexFeature[], Function<Attribute, Index>>> getIndexMatrix() {
        return Arrays.asList(
                Triplet.with("Hash", new IndexFeature[]{EQ, IN, QZ}, (Function<Attribute, Index>) HashIndex::onAttribute),
                Triplet.with("Unique", new IndexFeature[]{UNIQUE, EQ, IN}, (Function<Attribute, Index>) HashIndex::onAttribute),
                Triplet.with("Compound", new IndexFeature[]{COMPOUND, EQ, IN, QZ}, (Function<Attribute, Index>) HashIndex::onAttribute),
                Triplet.with("Navigable", new IndexFeature[]{EQ, IN, QZ, LT, GT, BT}, (Function<Attribute, Index>) NavigableIndex::onAttribute),
                Triplet.with("RadixTree", new IndexFeature[]{EQ, IN, SW}, (Function<Attribute, Index>) RadixTreeIndex::onAttribute),
                Triplet.with("ReversedRadixTree", new IndexFeature[]{EQ, IN, EW}, (Function<Attribute, Index>) ReversedRadixTreeIndex::onAttribute),
                Triplet.with("InvertedRadixTree", new IndexFeature[]{EQ, IN, CI}, (Function<Attribute, Index>) InvertedRadixTreeIndex::onAttribute),
                Triplet.with("SuffixTree", new IndexFeature[]{EQ, IN, EW, SC}, (Function<Attribute, Index>) SuffixTreeIndex::onAttribute)
        );
    }

    @Override
    public <O> IndexedCollection<O> getIndexedCollection(Class<O> klass) {
        IndexedCollection existingCollection = indexedCollections.get(klass.getName());
        if (existingCollection == null) {
            ConcurrentIndexedCollection<O> indexedCollection = new ConcurrentIndexedCollection<>();
            indexedCollections.put(klass.getName(), indexedCollection);
            return indexedCollection;
        } else {
            return existingCollection;
        }
    }

}
