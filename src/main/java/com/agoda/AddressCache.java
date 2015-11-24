package com.agoda;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/*
 * The AddressCache has a max age for the elements it's storing, an add method
 * for adding elements, a remove method for removing, a peek method which
 * returns the most recently added element, and a take method which removes
 * and returns the most recently added element.
 */
public class AddressCache {

    private static final Logger log = LogManager.getLogger(AddressCache.class);

    //time interval for invalidating cache in milliseconds
    private static final long INVALIDATING_INTERVAL = 50L;

    //time interval for cleaning cache in seconds
    private static final long CLEANING_INTERVAL = 1L;

    private final long EXPIRATION;
    private final LinkedHashMap<InetAddress, CacheEntry> innerStorage;
    private final ReentrantReadWriteLock lock;
    private final ScheduledExecutorService invalidatingService;
    private final ScheduledExecutorService cleaningService;

    public AddressCache(long maxAge, TimeUnit unit) {
        if (maxAge < 1L) throw new IllegalArgumentException("maxAge must be positive number!");
        if (unit == null) throw new IllegalArgumentException("time unit must be specified!");

        this.EXPIRATION = unit.toMillis(maxAge);
        this.innerStorage = new LinkedHashMap<>();
        this.lock = new ReentrantReadWriteLock();

        CacheCleaner cleaner = new CacheCleaner(this.lock, this.innerStorage);
        CacheValidator validator = new CacheValidator(this.lock, this.innerStorage);

        //execute cache validating task with specified delay
        invalidatingService = Executors.newScheduledThreadPool(1);
        invalidatingService.scheduleWithFixedDelay(validator, 0, INVALIDATING_INTERVAL, TimeUnit.MILLISECONDS);

        //execute cache cleaner task with specified delay
        cleaningService = Executors.newScheduledThreadPool(1);
        cleaningService.scheduleWithFixedDelay(cleaner, 0, CLEANING_INTERVAL, TimeUnit.SECONDS);
    }

    /**
     * add() method must store unique elements only (existing elements must be ignored).
     * This will return true if the element was successfully added.
     *
     * @param address
     * @return
     */
    public boolean add(InetAddress address) {
        if (address == null) return false;

        boolean result = false;

        try {
            lock.readLock().lock();
            if (!innerStorage.containsKey(address) || !innerStorage.get(address).isValid()) {
                //unlock read lock before acquiring write lock
                lock.readLock().unlock();
                //there some other thread may change entry state
                lock.writeLock().lock();
                try {
                    //double check!
                    boolean contains = innerStorage.containsKey(address);
                    if (!contains || !innerStorage.get(address).isValid()) {
                        final long newExpiration = System.currentTimeMillis() + EXPIRATION;
                        if (!contains) {
                            //put new entry
                            innerStorage.put(address, new CacheEntry(newExpiration, address));
                        } else {
                            //update expiration for existing but expired entry
                            innerStorage.get(address).setExpiration(newExpiration);
                        }
                        result = true;
                        log.debug("address " + address + " was added to cache");
                    } else { //case when storage contains entry and it is not expired
                        log.debug("address " + address + " is already in cache");
                    }
                    //downgrade to read lock
                    lock.readLock().lock();
                } finally {
                    lock.writeLock().unlock();

                }
            } else {
                log.debug("address " + address + " is already in cache");
            }
        } finally {
            lock.readLock().unlock();
        }

        return result;
    }

    /**
     * remove() method will return true if the address was successfully removed
     *
     * @param address
     * @return
     */
    public boolean remove(InetAddress address) {
        if (address == null) return false;

        boolean result = false;

        lock.readLock().lock();
        try {
            if (innerStorage.containsKey(address) && innerStorage.get(address).isValid()) {
                //unlock read lock before acquiring write lock
                lock.readLock().unlock();
                //there some other thread may change entry state
                lock.writeLock().lock();
                try {
                    //double check
                    if (innerStorage.containsKey(address) && innerStorage.get(address).isValid()) {
                        //delete existing and not expired entry
                        innerStorage.remove(address);
                        result = true;
                        log.debug("address " + address + " was removed from the cache");
                    } else {
                        //do nothing here - cache cleaner will remove expired entry
                        log.debug("address " + address + " is not found in the cache");
                    }
                    //downgrade to read lock
                    lock.readLock().lock();
                } finally {
                    lock.writeLock().unlock();
                }
            } else {
                log.debug("address " + address + " is not found in the cache");
            }
        } finally {
            lock.readLock().unlock();
        }

        return result;
    }

    /**
     * The peek() method will return the most recently added element,
     * null if no element exists.
     *
     * @return
     */
    public InetAddress peek() {
        InetAddress result = null;

        lock.readLock().lock();
        try {
            result = getFirstValidEntry(innerStorage);
            //if cache is empty or doesn't contain not expired entries
            if (innerStorage.isEmpty() || result == null) {
                log.debug("cache is empty");
            } else {
                //if valid entry was found
                log.debug("peeked address " + result);
            }
        } finally {
            lock.readLock().unlock();
        }

        return result;
    }

    /**
     * take() method retrieves and removes the most recently added element
     * from the cache and waits if necessary until an element becomes available.
     *
     * @return
     */
    public InetAddress take() {
        InetAddress result = null;

        lock.readLock().lock();
        try {
            //maybe the second condition is not a good idea here..
            if (innerStorage.isEmpty() || getFirstValidEntry(innerStorage) == null) {
                log.debug("cache is empty");
            } else {
                //unlock read lock before acquiring write lock
                lock.readLock().unlock();
                //there some other thread may change entry state
                lock.writeLock().lock();
                try {
                    InetAddress key = getFirstValidEntry(innerStorage);
                    //double check
                    if (innerStorage.isEmpty() || key == null) {
                        log.debug("cache is empty");
                    } else {
                        result = innerStorage.remove(key).getInetAddress();
                        log.debug("taken address " + result + " was removed from the cache");
                    }
                    //downgrade to read lock
                    lock.readLock().lock();
                } finally {
                    lock.writeLock().unlock();
                }
            }
        } finally {
            lock.readLock().unlock();
        }

        return result;
    }

    private InetAddress getFirstValidEntry(LinkedHashMap<InetAddress, CacheEntry> storage) {
        InetAddress result = null;

        for (Map.Entry<InetAddress, CacheEntry> inetAddressCacheEntryEntry : storage.entrySet()) {
            CacheEntry entry = inetAddressCacheEntryEntry.getValue();
            if (entry.isValid()) {
                result = entry.getInetAddress();
                break;
            }
        }

        return result;
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        //stop executor services
        cleaningService.shutdownNow();
        invalidatingService.shutdownNow();
    }

    static class CacheEntry {
        private final InetAddress inetAddress;
        private long expiration;
        private boolean isValid;

        CacheEntry(long expiration, InetAddress inetAddress) {
            this.expiration = expiration;
            this.inetAddress = inetAddress;
            this.isValid = true;
        }

        InetAddress getInetAddress() {
            return inetAddress;
        }

        long getExpiration() {
            return expiration;
        }

        void setExpiration(long expiration) {
            this.expiration = expiration;
        }

        public boolean isValid() {
            return isValid;
        }

        void invalidate() {
            long now = System.currentTimeMillis();
            if (expiration < now) this.isValid = false;
        }
    }

    static class CacheCleaner implements Runnable {

        private ReentrantReadWriteLock lock;
        private Map<InetAddress, CacheEntry> storage;

        public CacheCleaner(ReentrantReadWriteLock lock, Map<InetAddress, CacheEntry> storage) {
            this.lock = lock;
            this.storage = storage;
        }

        @Override
        public void run() {
            lock.writeLock().lock();
            try {
                clean(storage);
            } finally {
                lock.writeLock().unlock();
            }
        }

        /**
         * Deletes expired entries from storage
         *
         * @param storage cache storage
         */
        private void clean(Map<InetAddress, CacheEntry> storage) {
            if (storage == null || storage.isEmpty()) return;

            long now = System.currentTimeMillis();
            int deletedCount = 0;

            Iterator<Map.Entry<InetAddress, CacheEntry>> it = storage.entrySet().iterator();

            //loop through cache
            while (it.hasNext()) {
                Map.Entry<InetAddress, CacheEntry> entry = it.next();

                //remove cache entry if expiration time is lesser than current time
                if (!entry.getValue().isValid()) {
                    it.remove();
                    deletedCount++;
                }
            }

            log.debug(deletedCount + " cache entries were removed");
        }
    }

    static class CacheValidator implements Runnable {

        private ReentrantReadWriteLock lock;
        private Map<InetAddress, CacheEntry> storage;

        public CacheValidator(ReentrantReadWriteLock lock, Map<InetAddress, CacheEntry> storage) {
            this.lock = lock;
            this.storage = storage;
        }

        @Override
        public void run() {
            try {
                lock.writeLock().lock();

                //check each cache entry whether its expired or not
                storage.values().forEach(CacheEntry::invalidate);
            } finally {
                lock.writeLock().unlock();
            }
        }
    }

    public static void main(String[] args) throws UnknownHostException, InterruptedException {
        final AddressCache cache = new AddressCache(1L, TimeUnit.SECONDS);
        InetAddress sample = InetAddress.getByName("www.agoda.com");

        List<InetAddress> sampleList = Arrays.asList(
                InetAddress.getByName("www.agoda.com"),
                InetAddress.getByName("www.google.com"),
                InetAddress.getByName("www.yandex.ru"),
                InetAddress.getByName("www.facebook.com"),
                InetAddress.getByName("www.bbc.com"),
                InetAddress.getByName("www.vk.com"),
                InetAddress.getByName("www.ya.ru"),
                InetAddress.getByName("www.ringcentral.com"),
                InetAddress.getByName("www.yahoo.com"),
                InetAddress.getByName("www.ebay.com"),
                InetAddress.getByName("www.steam.com"),
                InetAddress.getByName("www.wikipedia.org")
        );

        ScheduledExecutorService writerService = Executors.newScheduledThreadPool(1);
        ScheduledExecutorService readerService = Executors.newScheduledThreadPool(1);

        writerService.scheduleWithFixedDelay((Runnable) () -> cache.add(sample), 0L, 2000L, TimeUnit.MILLISECONDS);
        readerService.scheduleWithFixedDelay((Runnable) () -> System.out.println(cache.peek()), 500L, 1000L, TimeUnit.MILLISECONDS);

        Thread.sleep(10000);
        writerService.shutdownNow();
        readerService.shutdownNow();
    }
}