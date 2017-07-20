package org.apache.ignite.cache.websession;

import java.io.Serializable;
import java.util.Map;

import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryRemovedListener;

import org.apache.ignite.internal.websession.WebSessionEntity;
import org.apache.ignite.marshaller.Marshaller;

/**
 * On session being removed from the cache, notify its attributes that they are being unbound from the session
 * 
 * @author ygou
 *
 * @param <K>
 * @param <V>
 */
public class WebSessionCacheEntryRemovedListener<K, V> implements CacheEntryRemovedListener<String, WebSessionEntity>, Serializable {
    private static final long serialVersionUID = 1L;
    
    private final Marshaller marshaller;
    
    public static void removeAttributes(Iterable<CacheEntryEvent<? extends String, ? extends WebSessionEntity>> events, final Marshaller marshaller) {
        if (events == null) {
            return;
        }
        
        events.forEach((e)->{
            WebSessionEntity webSessionEntity = e.getValue();
            if (webSessionEntity == null) {
                return;
            }
            
            WebSessionV2 webSessionV2 = new WebSessionV2(webSessionEntity, marshaller);            
            Map<String, byte[]> attrs = webSessionEntity.attributes();
            if (attrs == null) {
                return;
            }
            
            attrs.forEach((name, value) -> {
                webSessionV2.removeAttribute(name);
            });
        });
    }

    @Override
    public void onRemoved(Iterable<CacheEntryEvent<? extends String, ? extends WebSessionEntity>> events) throws CacheEntryListenerException {
        removeAttributes(events, marshaller);
    }

    public WebSessionCacheEntryRemovedListener(final Marshaller marshaller) {
        this.marshaller = marshaller;
    }    
}
