package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.junit.Test;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;

public class TestImageServlet {

    @Test
    // Test if getThrottler creates a DataTransferThrottler instance when a valid non-zero bandwidth is configured.
    // 1. Use the correct HDFS 2.8.5 API to obtain configuration values, avoiding hardcoding configuration values.
    // 2. Set up test conditions, such as the correct value for dfs.image.transfer.bandwidthPerSec.
    // 3. Execute the getThrottler method and verify the returned results.
    public void test_getThrottler_withValidConfiguration() {
        // Step 1: Prepare the test conditions by creating a Configuration object
        Configuration conf = new Configuration();
        long testBandwidth = 1048576L; // 1MB per second as bandwidth limit
        conf.setLong(DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_KEY, testBandwidth);

        // Step 2: Call the getThrottler method with the prepared Configuration object
        DataTransferThrottler throttler = ImageServlet.getThrottler(conf);

        // Step 3: Assert and verify results
        // Verify that the returned DataTransferThrottler instance is not null
        assertNotNull("Expected DataTransferThrottler instance, but got null.", throttler);

        // Verify that the bandwidth of the created throttler equals the configured bandwidth
        assertEquals("Throttler bandwidth does not match configured value.",
                testBandwidth, throttler.getBandwidth());
    }
}