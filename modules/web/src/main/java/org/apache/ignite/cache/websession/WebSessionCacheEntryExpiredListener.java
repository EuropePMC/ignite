package org.apache.ignite.cache.websession;

import java.io.Serializable;

import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryExpiredListener;
import javax.cache.event.CacheEntryListenerException;

import org.apache.ignite.internal.websession.WebSessionEntity;
import org.apache.ignite.marshaller.Marshaller;

/**
 * On session expiration, notify the attributes that they are being unbound from the session  
 * 
 * @author ygou
 *
 * @param <K>
 * @param <V>
 */
public class WebSessionCacheEntryExpiredListener<K, V> implements CacheEntryExpiredListener<String, WebSessionEntity>, Serializable {
    private static final long serialVersionUID = 1L;
    
    private final Marshaller marshaller;

    @Override
    public void onExpired(Iterable<CacheEntryEvent<? extends String, ? extends WebSessionEntity>> events) throws CacheEntryListenerException {
        WebSessionCacheEntryRemovedListener.removeAttributes(events, marshaller);
    }

    public WebSessionCacheEntryExpiredListener(Marshaller marshaller) {
        this.marshaller = marshaller;
    }
}
