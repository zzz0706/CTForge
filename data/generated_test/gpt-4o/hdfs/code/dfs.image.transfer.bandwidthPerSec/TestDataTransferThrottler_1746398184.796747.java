package org.apache.hadoop.hdfs.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.hdfs.util.Canceler;
import org.junit.Test;

import java.lang.reflect.Field;

import static org.junit.Assert.assertTrue;

public class TestDataTransferThrottler {

    @Test
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testThrottlePeriodResetAfterLongInactivity() throws Exception {
        // 1. Prepare the configuration and obtain the value using the API
        Configuration conf = new Configuration();
        conf.setLong("dfs.image.transfer.bandwidthPerSec", 1024); // Example of setting throttler bandwidth dynamically
        long bandwidthPerSec = conf.getLong("dfs.image.transfer.bandwidthPerSec", 1024); // Use API to fetch configuration values
        DataTransferThrottler throttler = new DataTransferThrottler(bandwidthPerSec);

        long inactivityPeriod = 3000; // Simulate long inactivity period in milliseconds
        long numOfBytes = 512; // Simulating a smaller data transfer for testing
        Canceler canceler = new Canceler();

        // Ensure throttler is not null (throttling is enabled)
        assertTrue(throttler != null);

        // 2. Perform initial throttling to update internal state
        throttler.throttle(numOfBytes, canceler);

        // 3. Simulate a long inactivity period by modifying the internal curPeriodStart using reflection
        Field curPeriodStartField = DataTransferThrottler.class.getDeclaredField("curPeriodStart");
        curPeriodStartField.setAccessible(true);
        long initialPeriodStart = (long) curPeriodStartField.get(throttler);
        curPeriodStartField.set(throttler, initialPeriodStart - inactivityPeriod);

        // 4. Call throttling again to test period reset logic
        throttler.throttle(numOfBytes, canceler);

        // Verify that curPeriodStart has been reset
        long updatedPeriodStart = (long) curPeriodStartField.get(throttler);
        assertTrue(updatedPeriodStart > initialPeriodStart);

        // Code after testing: Ensure cleanup if necessary (e.g., resetting any modified fields)
        curPeriodStartField.set(throttler, initialPeriodStart); // Reset curPeriodStart for future tests
    }
}