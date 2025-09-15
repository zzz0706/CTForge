package org.apache.hadoop.hdfs.server.namenode.top;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.top.TopConf;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertArrayEquals;

public class TestTopConf {

    @Test
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testLargeValuesConfiguration() {
        // Step 1: Prepare Configuration object and set configuration values
        Configuration conf = new Configuration();
        conf.setStrings(DFSConfigKeys.NNTOP_WINDOWS_MINUTES_KEY, "60", "1440", "10080"); // Set the top windows reporting periods in minutes

        // Step 2: Instantiate TopConf with the prepared Configuration
        TopConf topConf = new TopConf(conf);

        // Step 3: Verify that the nntopReportingPeriodsMs array contains correct millisecond values
        int[] expectedValues = new int[]{
            (int) TimeUnit.MINUTES.toMillis(60),      // 60 minutes in milliseconds
            (int) TimeUnit.MINUTES.toMillis(1440),    // 1440 minutes in milliseconds
            (int) TimeUnit.MINUTES.toMillis(10080)    // 10080 minutes in milliseconds
        };

        assertArrayEquals(
            "The nntopReportingPeriodsMs array should contain the correct millisecond values.",
            expectedValues,
            topConf.nntopReportingPeriodsMs
        );

        // Step 4: Additional testing (if needed)
        // You can verify other properties of the TopConf class here if necessary.
    }
}