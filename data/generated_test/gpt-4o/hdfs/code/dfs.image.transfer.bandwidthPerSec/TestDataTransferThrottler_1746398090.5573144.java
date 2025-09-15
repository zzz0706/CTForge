package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.hdfs.util.Canceler;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit test class to validate the behavior of ImageServlet and DataTransferThrottler.
 */
public class TestDataTransferThrottler {

    @Test
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testThrottleExceedsLimitSleepBehavior() throws InterruptedException {
        // 1. Obtain configuration values using the HDFS API.
        Configuration conf = new Configuration();
        conf.setLong(DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_KEY, 1024L * 1024L); // Set using configuration key
        long bandwidthPerSec = conf.getLong(DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_KEY, DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_DEFAULT);

        // If bandwidth is not set properly, return and skip the test.
        if (bandwidthPerSec <= 0) {
            return;
        }

        // Initialize the DataTransferThrottler with the obtained bandwidth.
        DataTransferThrottler throttler = new DataTransferThrottler(bandwidthPerSec);

        // 2. Prepare the test conditions.
        long dataToTransfer = bandwidthPerSec * 2; // Simulate data exceeding limit.
        long startTime = System.currentTimeMillis();

        // Mock a canceler for testing.
        Canceler canceler = Mockito.mock(Canceler.class);
        Mockito.when(canceler.isCancelled()).thenReturn(false);

        // 3. Test code.
        throttler.throttle(dataToTransfer, canceler);
        long endTime = System.currentTimeMillis();
        long elapsedTime = endTime - startTime;

        // Calculate the expected sleep time for throttling.
        long expectedSleepMillis = (dataToTransfer - bandwidthPerSec) / (bandwidthPerSec / 1000);

        // 4. Code after testing.
        Assert.assertTrue(
            "Throttle should enforce sleep when data transfer exceeds bandwidth limit.",
            elapsedTime >= expectedSleepMillis
        );
    }

    @Test
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions using public methods.
    // 3. Test code.
    // 4. Code after testing.
    public void testGetThrottlerConfiguration() {
        // 1. Obtain configuration values using the HDFS API.
        Configuration conf = new Configuration();
        conf.setLong(DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_KEY, 2048L * 1024L); // Set bandwidth using configuration key

        // 2. Use public methods to prepare and verify the output.
        DataTransferThrottler throttler = ImageServlet.getThrottler(conf);

        // 3. Test code - validate that the throttler is correctly initialized based on configuration.
        Assert.assertNotNull("Throttler should not be null when a valid bandwidth configuration is provided.", throttler);
        Assert.assertEquals(
            "Throttler should use the configured bandwidth limit.",
            2048L * 1024L,
            throttler.getBandwidth()
        );

        // 4. Code after testing - ensure proper behavior in edge cases.
        conf.setLong(DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_KEY, 0);
        throttler = ImageServlet.getThrottler(conf);
        Assert.assertNull("Throttler should be null when bandwidth configuration is zero or invalid.", throttler);
    }
}