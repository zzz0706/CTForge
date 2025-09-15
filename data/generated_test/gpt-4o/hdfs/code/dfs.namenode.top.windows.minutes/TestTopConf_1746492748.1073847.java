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
    // 3. Verify correct behavior of `TopConf(Configuration conf)` using large values for configuration.
    // 4. Code after testing.
    public void testLargeValuesConfiguration() {
        // Step 1: Prepare a Configuration object and set the configuration value using the HDFS 2.8.5 API
        Configuration conf = new Configuration();
        conf.setStrings(DFSConfigKeys.NNTOP_WINDOWS_MINUTES_KEY, "60", "1440", "10080"); // Define the configuration key with valid large values
        
        // Step 2: Instantiate the TopConf class with the prepared Configuration object
        TopConf topConf = new TopConf(conf);

        // Step 3: Verify that nntopReportingPeriodsMs contains the expected time periods in milliseconds
        int[] expectedValues = new int[]{
            (int) TimeUnit.MINUTES.toMillis(60),     // Convert 60 minutes to milliseconds
            (int) TimeUnit.MINUTES.toMillis(1440),   // Convert 1440 minutes to milliseconds
            (int) TimeUnit.MINUTES.toMillis(10080)   // Convert 10080 minutes to milliseconds
        };
        assertArrayEquals(
            "The nntopReportingPeriodsMs array should match the expected millisecond values.",
            expectedValues,
            topConf.nntopReportingPeriodsMs
        );

        // Step 4: Ensure original configuration retrieval is valid and matches expectations
        String[] configuredPeriodsStr = conf.getTrimmedStrings(DFSConfigKeys.NNTOP_WINDOWS_MINUTES_KEY);
        assertArrayEquals(
            "The configuration values should match the input values.",
            new String[]{"60", "1440", "10080"},
            configuredPeriodsStr
        );
    }
}