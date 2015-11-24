package com.agoda;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

/**
 *
 * Created by Andrey Kapitonov on 24.11.2015.
 */
public class TestAddressCache {

    private AddressCache cache;
    private InetAddress sample1;


    @Before
    public void setUp() throws Exception {
        cache = new AddressCache(1L, TimeUnit.SECONDS);
        sample1 = InetAddress.getByName("www.agoda.com");
    }

    @Test
    public void testSingleThreadPeek() throws Exception {
        cache.add(sample1);
        //wait
        Thread.sleep(500);
        //check if entry still in the cache
        assertThat(sample1, is(equalTo(cache.peek())));
        //wait
        Thread.sleep(600);
        //check if entry was removed from cache
        assertThat(cache.peek(), is(nullValue()));
    }

    @Test
    public void testSingleThreadRemove() throws Exception {
        //add sample
        cache.add(sample1);
        //check if remove works ok
        assertThat(Boolean.TRUE, is(equalTo(cache.remove(sample1))));
        //check if remove fails if entry doesn't exist
        assertThat(Boolean.FALSE, is(equalTo(cache.remove(sample1))));

        //add sample again
        cache.add(sample1);
        //wait until entry expired
        Thread.sleep(1100);
        //check if remove returns false
        assertThat(Boolean.FALSE, is(equalTo(cache.remove(sample1))));
    }
}
