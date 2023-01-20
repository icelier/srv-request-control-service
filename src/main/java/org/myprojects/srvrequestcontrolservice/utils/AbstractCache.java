package org.myprojects.srvrequestcontrolservice.utils;

public interface AbstractCache<T> {

    void cacheUnit(String unitKey, T cacheUnit);

    T getCachedUnit(String unitKey);

    void clearCachedUnit(String unitKey);
}
