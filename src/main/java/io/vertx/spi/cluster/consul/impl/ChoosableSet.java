package io.vertx.spi.cluster.consul.impl;

import io.vertx.core.impl.ConcurrentHashSet;
import io.vertx.core.spi.cluster.ChoosableIterable;

import java.io.Serializable;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * @author <a href="http://tfox.org">Tim Fox</a>
 */
class ChoosableSet<T> implements ChoosableIterable<T>, Serializable {

    private final Set<T> ids;
    private volatile Iterator<T> iter;

    public ChoosableSet(int initialSize) {
        ids = new ConcurrentHashSet<>(initialSize);
    }

    public Set<T> getIds() {
        return ids;
    }

    public int size() {
        return ids.size();
    }

    public void add(T elem) {
        ids.add(elem);
    }

    public void remove(T elem) {
        ids.remove(elem);
    }

    public void merge(ChoosableSet<T> toMerge) {
        ids.addAll(toMerge.ids);
    }

    public boolean isEmpty() {
        return ids.isEmpty();
    }

    public boolean contains(T elem) {
        return ids.contains(elem);
    }

    @Override
    public Iterator<T> iterator() {
        return ids.iterator();
    }

    public synchronized T choose() {
        if (!ids.isEmpty()) {
            if (iter == null || !iter.hasNext()) {
                iter = ids.iterator();
            }
            try {
                return iter.next();
            } catch (NoSuchElementException e) {
                return null;
            }
        } else {
            return null;
        }
    }
}

