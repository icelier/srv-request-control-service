package org.myprojects.srvrequestcontrolservice.utils;

import lombok.extern.slf4j.Slf4j;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Iterator;
import java.util.Map;

@Slf4j
public class TempCache<T extends TempCache.Unit> extends SimpleCache<T> {

    // 15 min
    private static final long CACHE_PERIOD_DEFAULT = 900000L;

    private final long cachePeriod;

    public TempCache(String name) {
        super(name);
        this.cachePeriod = CACHE_PERIOD_DEFAULT;
    }

    public TempCache(String name, long cachePeriod) {
        super(name);
        this.cachePeriod = cachePeriod;
    }

    public long getCachePeriod() {
        return this.cachePeriod;
    }

    @Override
    public void cacheUnit(String unitKey, T cacheUnit) {
        log.info(this.name + ": cache unit with messageId " + unitKey);
        cache.put(unitKey, cacheUnit);
    }

    @Override
    public T getCachedUnit(String unitKey) {
        Iterator<Map.Entry<String, T>> iter = cache.entrySet().iterator();

        Map.Entry<String, T> next = null;
        while (iter.hasNext()) {
            next = iter.next();
            if (next.getKey().equals(unitKey)) {
                next.getValue().setAccessTimestamp(LocalDateTime.now());
            } else {
                next = null;
            }
        }

        return next == null ? null : next.getValue();
    }

    public void cleanExpiredCache() {
        Iterator<Map.Entry<String, T>> iterator = cache.entrySet().iterator();
        LocalDateTime now = LocalDateTime.now();

        LocalDateTime lastAccessTime;
        Map.Entry<String, T> entry;
        while(iterator.hasNext()) {
            entry = iterator.next();
            lastAccessTime = entry.getValue().getLastAccessedAt().plus(this.cachePeriod, ChronoUnit.MILLIS);
            if (lastAccessTime.isBefore(now)) {
                log.info(this.name + ": remove unit with messageId " + entry.getKey());
                iterator.remove();
            }
        }
    }

    public static class Unit<T> {

        private T cacheUnit;
        private LocalDateTime lastAccessedAt;

        public Unit(T cacheUnit) {
            this.cacheUnit = cacheUnit;
            this.lastAccessedAt = LocalDateTime.now();
        }

        public T getCacheUnit() {
            return cacheUnit;
        }

        public void setAccessTimestamp(LocalDateTime timestamp) {
            this.lastAccessedAt = timestamp;
        }

        public LocalDateTime getLastAccessedAt() {
            return this.lastAccessedAt;
        }
    }
}
