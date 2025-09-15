package org.apache.hadoop.hdfs.server.namenode.top;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.namenode.top.TopConf;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.junit.Test;
import static org.junit.Assert.assertArrayEquals;

import java.util.concurrent.TimeUnit;  // Correct import for TimeUnit

public class TestTopConf {

    @Test
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testLargeValuesConfiguration() {
        // 1. Prepare Configuration object and set configuration values
        Configuration conf = new Configuration();
        conf.setStrings("dfs.namenode.top.windows.minutes", "60", "1440", "10080"); // 1 hour, 1 day, 1 week in minutes

        // 2. Instantiate TopConf with the prepared Configuration
        TopConf topConf = new TopConf(conf);

        // 3. Verify that the nntopReportingPeriodsMs array contains correct millisecond values
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
    }
}