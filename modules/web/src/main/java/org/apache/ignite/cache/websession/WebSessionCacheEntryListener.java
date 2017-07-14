package org.apache.ignite.cache.websession;

import java.io.Serializable;

import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryExpiredListener;
import javax.cache.event.CacheEntryListenerException;

public class WebSessionCacheEntryListener<K, V> implements CacheEntryExpiredListener<K, V>, Serializable {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    @Override
    public void onExpired(Iterable<CacheEntryEvent<? extends K, ? extends V>> events) throws CacheEntryListenerException {
        // TODO Auto-generated method stub
        
    }

}
