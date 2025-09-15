package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.util.Time;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestImageServlet {

    @Test
    // testThrottleUnderLimit
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
        long numOfBytes = bandwidth / 2; // Use a number of bytes less than the bandwidth limit.

        // Simulate throttling behavior and record execution time.
        long startTime = System.nanoTime();
        throttler.throttle(numOfBytes, null); // Passing null as Canceler is not canceled.
        long endTime = System.nanoTime();

        // 3. Test code.
        long expectedDelayMillis = (numOfBytes * 1000) / bandwidth; // Calculate expected delay.
        long elapsedMillis = (endTime - startTime) / 1_000_000; // Convert elapsed time to milliseconds.

        // Assert that there is no thread sleep or delay below the threshold.
        assertTrue(
            "Throttle method failed when under limit. Elapsed time: " + elapsedMillis + " ms, Expected: <= " + expectedDelayMillis + " ms",
            elapsedMillis <= expectedDelayMillis + 5 // Add a small overhead tolerance.
        );

        // 4. Code after testing.
        // No additional cleanup necessary for this test case.
    }

    @Test
    // testGetThrottler
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testGetThrottler() {
        // 1. Obtain the configuration values using the Hadoop configuration API.
        Configuration conf = new Configuration();
        conf.setLong("dfs.image.transfer.bandwidthPerSec", 2048); // Set bandwidth configuration.

        // 2. Prepare the test conditions.
        DataTransferThrottler throttler = ImageServlet.getThrottler(conf);
        assertNotNull("DataTransferThrottler should not be null when a valid bandwidth is set.", throttler);

        long expectedBandwidth = conf.getLong("dfs.image.transfer.bandwidthPerSec", 0);
        assertTrue("Throttler bandwidth should match the configured value.", throttler.getBandwidth() == expectedBandwidth);

        // 3. Test code.
        long numOfBytes = 1024; // Test with bytes below the configured limit.
        long startTime = System.nanoTime();
        throttler.throttle(numOfBytes, null); // Passing null as Canceler is not in use.
        long endTime = System.nanoTime();

        long expectedDelayMillis = (numOfBytes * 1000) / expectedBandwidth; // Calculate the allowable delay.
        long elapsedMillis = (endTime - startTime) / 1_000_000; // Convert elapsed time to milliseconds.

        // Verify throttling behavior for the configured bandwidth.
        assertTrue(
            "Throttle method behavior incorrect when under limit. Elapsed time: " + elapsedMillis + " ms, Expected: <= " + expectedDelayMillis + " ms",
            elapsedMillis <= expectedDelayMillis + 5 // Add a small margin for overhead.
        );

        // 4. Code after testing.
        // No additional cleanup necessary for this test case.
    }
}