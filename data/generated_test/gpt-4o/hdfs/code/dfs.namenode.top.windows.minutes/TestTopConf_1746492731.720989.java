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
        // Step 1: Prepare Configuration object and set configuration values using the HDFS 2.8.5 API
        Configuration conf = new Configuration();
        conf.setStrings(DFSConfigKeys.NNTOP_WINDOWS_MINUTES_KEY, "60", "1440", "10080"); // Setting the configuration key for top windows reporting periods

        // Step 2: Instantiate the TopConf class using the above configuration
        TopConf topConf = new TopConf(conf);

        // Step 3: Verify that the nntopReportingPeriodsMs array contains the expected millisecond values
        int[] expectedValues = new int[]{
            (int) TimeUnit.MINUTES.toMillis(60),      // 60 minutes in milliseconds (1 hour)
            (int) TimeUnit.MINUTES.toMillis(1440),    // 1440 minutes in milliseconds (1 day)
            (int) TimeUnit.MINUTES.toMillis(10080)    // 10080 minutes in milliseconds (1 week)
        };

        assertArrayEquals(
            "The nntopReportingPeriodsMs array should contain the correct millisecond values.",
            expectedValues,
            topConf.nntopReportingPeriodsMs
        );

        // Step 4: Additional validation for configuration propagation and internal correctness
        String[] configuredPeriodsStr = conf.getTrimmedStrings(DFSConfigKeys.NNTOP_WINDOWS_MINUTES_KEY);
        assertArrayEquals(
            "The configuration values should match the input values.",
            new String[]{"60", "1440", "10080"},
            configuredPeriodsStr
        );
    }
}