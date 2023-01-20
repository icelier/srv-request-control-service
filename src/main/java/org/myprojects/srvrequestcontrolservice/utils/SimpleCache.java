package org.myprojects.srvrequestcontrolservice.utils;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SimpleCache<T> implements AbstractCache<T> {

    protected String name;

    protected Map<String, T> cache = new ConcurrentHashMap<>();

    public SimpleCache(String name) {
        this.name = name;
    }

    @Override
    public void cacheUnit(String unitKey, T cacheUnit) {
        cache.put(unitKey, cacheUnit);
    }

    @Override
    public T getCachedUnit(String unitKey) {
        Iterator<Map.Entry<String, T>> iter = cache.entrySet().iterator();

        Map.Entry<String, T> next;
        while (iter.hasNext()) {
            next = iter.next();
            if (next.getKey().equals(unitKey)) {
                return next.getValue();
            }
        }

        return null;
    }

    @Override
    public void clearCachedUnit(String unitKey) {
        Iterator<Map.Entry<String, T>> iter = cache.entrySet().iterator();

        Map.Entry<String, T> next;
        while (iter.hasNext()) {
            next = iter.next();
            if (next.getKey().equals(unitKey)) {
                iter.remove();
            }
        }
    }

    public void clearCachedUnitExcept(List<String> effectiveKeys) {
        Iterator<Map.Entry<String, T>> iter = cache.entrySet().iterator();

        Map.Entry<String, T> next;
        while (iter.hasNext()) {
            next = iter.next();
            if (!effectiveKeys.contains(next.getKey())) {
                iter.remove();
            }
        }
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getSize() {
        return this.cache.size();
    }
}
