package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.util.Time;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Unit test for verifying the proper usage and behavior of DataTransferThrottler
 * and configuration in ImageServlet.
 */
public class TestImageServlet {

    @Test
    // test code for testThrottleUnderLimit
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testThrottleUnderLimit() throws InterruptedException {
        // 1. Obtain the configuration values using the Hadoop configuration API.
        Configuration conf = new Configuration();
        conf.setLong("dfs.image.transfer.bandwidthPerSec", 1024); // Define the bandwidth as per test case.

        // Use the static method from ImageServlet to construct the throttler.
        DataTransferThrottler throttler = ImageServlet.getThrottler(conf);
        assertNotNull("DataTransferThrottler should not be null when valid bandwidth is configured.", throttler);

        long bandwidth = throttler.getBandwidth();
        assertTrue("Bandwidth should be greater than 0.", bandwidth > 0);

        // 2. Prepare the test conditions.
        // Work with a number of bytes less than the bandwidth limit.
        long numOfBytes = bandwidth / 2;

        // Simulate throttling behavior and measure execution time.
        long startTime = System.nanoTime();
        throttler.throttle(numOfBytes, null); // Passing null as Canceler is not canceled.
        long endTime = System.nanoTime();

        // 3. Test code.
        // Calculate the expected delay in milliseconds for this amount of data.
        long expectedDelayMillis = (numOfBytes * 1000) / bandwidth;

        // Verify that the actual delay is within an acceptable range (allowing some margin for overhead).
        long elapsedMillis = (endTime - startTime) / 1_000_000; // Convert elapsed time to milliseconds.
        assertTrue(
            "Throttle method failed when under limit. Elapsed time: " + elapsedMillis + " ms, Expected: <= " + expectedDelayMillis + " ms",
            elapsedMillis <= expectedDelayMillis + 5 // Add small overhead tolerance.
        );

        // 4. Code after testing.
        // No additional cleanup necessary for this test case.
    }

    @Test
    // test code for ImageServlet.getThrottler
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testGetThrottler() {
        // 1. Obtain the configuration values using the Hadoop configuration API.
        Configuration conf = new Configuration();
        conf.setLong("dfs.image.transfer.bandwidthPerSec", 2048); // Set bandwidth for the test.

        // 2. Prepare the test conditions.
        // Use ImageServlet to get the throttler instance.
        DataTransferThrottler throttler = ImageServlet.getThrottler(conf);
        assertNotNull("DataTransferThrottler should not be null when a valid bandwidth is set.", throttler);

        // Verify that the bandwidth is correctly initialized.
        long expectedBandwidth = conf.getLong("dfs.image.transfer.bandwidthPerSec", 0);
        assertTrue("The throttler bandwidth should match the configuration value.", throttler.getBandwidth() == expectedBandwidth);

        // 3. Test code.
        // Test with some bytes to confirm the throttler behavior.
        long numOfBytes = 1024; // Half the configured bandwidth.
        long startTime = System.nanoTime();
        throttler.throttle(numOfBytes, null); // Passing null as Canceler is not canceled.
        long endTime = System.nanoTime();

        // Calculate the allowable delay.
        long expectedDelayMillis = (numOfBytes * 1000) / expectedBandwidth;

        // Verify the throttling result.
        long elapsedMillis = (endTime - startTime) / 1_000_000; // Convert elapsed time to milliseconds.
        assertTrue(
            "Throttle method failed when under limit. Elapsed time: " + elapsedMillis + " ms, Expected: <= " + expectedDelayMillis + " ms",
            elapsedMillis <= expectedDelayMillis + 5 // Add small overhead tolerance.
        );

        // 4. Code after testing.
        // No additional cleanup is necessary.
    }
}