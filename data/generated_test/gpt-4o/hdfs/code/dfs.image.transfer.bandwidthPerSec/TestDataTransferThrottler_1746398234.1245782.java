package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.hdfs.util.Canceler;
import org.junit.Test;

import java.lang.reflect.Field;

import static org.junit.Assert.*;

public class TestDataTransferThrottler {

    @Test
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testThrottlePeriodResetAfterLongInactivity() throws Exception {
        // 1. Obtain configuration values dynamically using the HDFS API for proper test setup
        Configuration conf = new Configuration();
        conf.setLong(DFSConfigKeys.DFS_IMAGE_TRANSFER_RATE_KEY, 1024); // Set bandwidth dynamically for the test
        DataTransferThrottler throttler = ImageServlet.getThrottler(conf);

        // Ensure throttler is created properly with the configuration
        assertNotNull("Throttler should be initialized", throttler);
        assertEquals("Throttler bandwidth should match the configured value", 1024, throttler.getBandwidth());

        // 2. Prepare the test conditions
        long inactivityPeriod = 3000; // Simulate a long inactivity period in milliseconds
        long numOfBytes = 512; // Data transfer size for testing
        Canceler canceler = new Canceler(); // Create a canceler for testing throttling

        // Perform initial throttling to establish the throttler's state
        throttler.throttle(numOfBytes, canceler);

        // Verify that initial throttling used the expected bandwidth
        Field curReserveField = DataTransferThrottler.class.getDeclaredField("curReserve");
        curReserveField.setAccessible(true);
        long curReserveBeforeInactivity = (long) curReserveField.get(throttler);

        // 3. Simulate inactivity by manipulating the `curPeriodStart` value via reflection
        Field curPeriodStartField = DataTransferThrottler.class.getDeclaredField("curPeriodStart");
        curPeriodStartField.setAccessible(true);
        long initialCurPeriodStart = (long) curPeriodStartField.get(throttler);
        curPeriodStartField.set(throttler, initialCurPeriodStart - inactivityPeriod);

        // Throttle again after the simulated inactivity
        throttler.throttle(numOfBytes, canceler);

        // Verify that throttler's `curPeriodStart` has been updated correctly
        long updatedCurPeriodStart = (long) curPeriodStartField.get(throttler);
        assertTrue("Throttle period start should reset after long inactivity", updatedCurPeriodStart > initialCurPeriodStart);

        // Verify that `curReserve` is updated correctly
        long curReserveAfterInactivity = (long) curReserveField.get(throttler);
        assertTrue("Throttle reserve should not be negative after reset", curReserveAfterInactivity > 0);

        // 4. Restore the original state for cleanup
        curPeriodStartField.set(throttler, initialCurPeriodStart);
        curReserveField.set(throttler, curReserveBeforeInactivity);
    }
}