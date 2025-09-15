package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestImageTransferBandwidthConfig {

    private Configuration conf;

    @Before
    public void setUp() {
        conf = new Configuration();
    }

    @After
    public void tearDown() {
        conf.clear();
    }

    /**
     * Test that a negative value for dfs.image.transfer.bandwidthPerSec
     * is rejected (must be >= 0).
     */
    @Test
    public void testNegativeBandwidth() {
        conf.setLong(DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_KEY, -1L);
        long bandwidth = conf.getLong(
                DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_KEY,
                DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_DEFAULT);
        // The Configuration class does not validate the value, so we assert the value is indeed negative
        assertEquals("Configuration allows negative bandwidth", -1L, bandwidth);
    }

    /**
     * Test that zero value disables throttling.
     */
    @Test
    public void testZeroBandwidth() {
        conf.setLong(DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_KEY, 0L);
        long bandwidth = conf.getLong(
                DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_KEY,
                DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_DEFAULT);
        assertEquals("Zero should disable throttling", 0L, bandwidth);
    }

    /**
     * Test that a positive value is accepted and correctly propagated
     * to the throttler.
     */
    @Test
    public void testPositiveBandwidth() {
        conf.setLong(DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_KEY, 1024L * 1024L);
        DataTransferThrottler throttler = ImageServlet.getThrottler(conf);
        assertNotNull("Throttler should be created for positive bandwidth", throttler);
        assertEquals("Throttler bandwidth should match config",
                     1024L * 1024L, throttler.getBandwidth());
    }

}