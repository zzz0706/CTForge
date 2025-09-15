package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.hdfs.util.Canceler;
import org.junit.Test;
import java.lang.reflect.Field;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestImageServlet {

    @Test
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testThrottlePeriodResetAfterLongInactivity() throws Exception {
        // 1. Obtain configuration values dynamically using the HDFS API for proper test setup
        Configuration conf = new Configuration();
        conf.setLong("dfs.image.transfer.bandwidthPerSec", 1024);
        DataTransferThrottler throttler = ImageServlet.getThrottler(conf);

        // Ensure throttler is created properly with the configuration
        assertNotNull(throttler);

        long inactivityPeriod = 3000; // Simulate long inactivity period in milliseconds
        long numOfBytes = 512; // Simulating a smaller data transfer for testing
        Canceler canceler = new Canceler();

        // 2. Perform initial throttling to establish the throttler's state
        throttler.throttle(numOfBytes, canceler);

        // 3. Simulate inactivity: manipulate the `curPeriodStart` value using reflection
        Field curPeriodStartField = DataTransferThrottler.class.getDeclaredField("curPeriodStart");
        curPeriodStartField.setAccessible(true);
        long initialCurPeriodStart = (long) curPeriodStartField.get(throttler);
        curPeriodStartField.set(throttler, initialCurPeriodStart - inactivityPeriod);

        // Call the throttler's `throttle` method again after the simulated inactivity
        throttler.throttle(numOfBytes, canceler);

        // Verify that throttler's `curPeriodStart` has been adjusted
        long updatedCurPeriodStart = (long) curPeriodStartField.get(throttler);
        assertTrue(updatedCurPeriodStart > initialCurPeriodStart);

        // 4. Restore the original `curPeriodStart` value for cleanup
        curPeriodStartField.set(throttler, initialCurPeriodStart);
    }
}