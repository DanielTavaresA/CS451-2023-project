package cs451.utils;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class AtomicSet<T> {

    private Set<T> set;
    private ReadWriteLock lock;

    public AtomicSet() {
        this.set = new HashSet<>();
        this.lock = new ReentrantReadWriteLock();
        ;

    }

    public AtomicSet(Set<T> set, ReadWriteLock lock) {
        this.set = set;
        this.lock = lock;
    }

    public boolean add(T t) {
        lock.writeLock().lock();
        boolean res = set.add(t);
        lock.writeLock().unlock();
        return res;
    }

    public boolean remove(T t) {
        lock.writeLock().lock();
        boolean res = set.remove(t);
        lock.writeLock().unlock();
        return res;
    }

    public boolean contains(T t) {
        lock.readLock().lock();
        boolean res = set.contains(t) ? true : false;
        lock.readLock().unlock();
        return res;
    }
}
