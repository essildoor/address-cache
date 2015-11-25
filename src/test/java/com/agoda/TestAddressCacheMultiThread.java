package com.agoda;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;

/**
 * Created by Andrey Kapitonov on 25.11.2015.
 */
public class TestAddressCacheMultiThread {
    private static final long MAX_AGE = 1000L;
    private static final TimeUnit TIME_UNIT = TimeUnit.MILLISECONDS;

    private AddressCache cache;
    private List<InetAddress> sampleAddresses;
    private ScheduledExecutorService writerService;
    private ScheduledExecutorService peekerService;
    private ScheduledExecutorService takerService;
    private ScheduledExecutorService removerService;

    private int numOfWriters = 1;
    private int numOfPeekers = 1;
    private int numOfTakers = 1;
    private int numOfRemovers = 1;

    @Before
    public void setUp() throws Exception {
        cache = new AddressCache(MAX_AGE, TIME_UNIT);
        sampleAddresses = new CopyOnWriteArrayList<InetAddress>() {{
            add(InetAddress.getByName("www.agoda.com"));
            add(InetAddress.getByName("www.google.com"));
            add(InetAddress.getByName("www.monster.com"));
        }};
        writerService = Executors.newScheduledThreadPool(numOfWriters);
        peekerService = Executors.newScheduledThreadPool(numOfPeekers);
        takerService = Executors.newScheduledThreadPool(numOfTakers);
        removerService = Executors.newScheduledThreadPool(numOfRemovers);
    }

    @After
    public void tearDown() throws Exception {
        peekerService.shutdown();
        takerService.shutdown();
        removerService.shutdown();
        //wait for taker service shutdowns
        writerService.shutdown();
        //wait for other services shutdown
        //delete cache ref
        cache = null;
        //call gc to clean up cache stuff
        System.gc();
    }

    @Test
    public void testTakeIsBlockingUntilEntryComes() throws Exception {
        ExecutorService takeTaskExecutorService = Executors.newSingleThreadExecutor();
        ScheduledExecutorService writerService = Executors.newScheduledThreadPool(1);

        FutureTask<InetAddress> takeTask = new FutureTask<>(() -> cache.take());

        //take task executor thread should get in wait state here until new entry will appears in the cache
        takeTaskExecutorService.execute(takeTask);

        System.out.println("[immediate] taker task is done: " + takeTask.isDone());

        Thread.sleep(200);

        System.out.println("[200ms] taker task is done: " + takeTask.isDone());

        //write an entry to the cache
        writerService.schedule((Runnable) () -> cache.add(sampleAddresses.get(0)), 500L, TimeUnit.MILLISECONDS);

        System.out.println("take task result: " + takeTask.get());

        takeTaskExecutorService.shutdown();
        writerService.shutdown();
    }

    @Test
    public void testOneWriterAndMultipleTakers() throws Exception {
        Random r = new Random();

        //one writer will write random entry each 100 ms
        writerService.scheduleWithFixedDelay(
                (Runnable) () -> cache.add(sampleAddresses.get(r.nextInt(sampleAddresses.size()))), 0L, 500L, TimeUnit.MILLISECONDS);

        takerService = Executors.newScheduledThreadPool(5);
        takerService.scheduleWithFixedDelay((Runnable) () -> System.out.println("taker thread # " + Thread.currentThread().getId() + " taken " + cache.take()), 0L, 300L, TimeUnit.MILLISECONDS);

        Thread.sleep(5000);
    }
}
