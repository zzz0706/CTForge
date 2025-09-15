package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.hdfs.util.Canceler;
import org.apache.hadoop.util.Time;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit test class to validate the behavior of DataTransferThrottler.
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
        conf.setLong("dfs.image.transfer.bandwidthPerSec", 1024 * 1024); // Set bandwidth using a valid configuration key
        long bandwidthPerSec = conf.getLong("dfs.image.transfer.bandwidthPerSec", 0);

        // If bandwidth is not set properly, skip the test.
        if (bandwidthPerSec <= 0) {
            return;
        }

        // Initialize the DataTransferThrottler with the obtained bandwidth.
        DataTransferThrottler throttler = new DataTransferThrottler(bandwidthPerSec);

        // 2. Prepare the test conditions.
        long dataToTransfer = bandwidthPerSec * 2; // Simulate data amount exceeding the bandwidth limit.
        long initialTime = Time.monotonicNow();

        // Mocking process for cancel checker using the Canceler class.
        Canceler canceler = Mockito.mock(Canceler.class);
        Mockito.when(canceler.isCancelled()).thenReturn(false);

        // 3. Test code.
        throttler.throttle(dataToTransfer, canceler);
        long finalTime = Time.monotonicNow();

        long elapsedTime = finalTime - initialTime;

        // Estimate expected sleep duration based on the bandwidth limit.
        long expectedSleepMillis = (dataToTransfer - bandwidthPerSec) / (bandwidthPerSec / 1000);

        // 4. Code after testing.
        Assert.assertTrue(
            "Throttle should enforce sleep when transfer data exceeds bandwidth limit.",
            elapsedTime >= expectedSleepMillis
        );
    }
}