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
        // Step 1: Use HDFS 2.8.5 API to set configuration values
        Configuration conf = new Configuration();
        conf.setStrings(DFSConfigKeys.NNTOP_WINDOWS_MINUTES_KEY, "60", "1440", "10080"); // Define valid large configuration values

        // Step 2: Prepare the test conditions by initializing TopConf with the Configuration object
        TopConf topConf = new TopConf(conf);

        // Step 3: Verify correct transformation of configuration values into nntopReportingPeriodsMs
        int[] expectedValues = new int[]{
            (int) TimeUnit.MINUTES.toMillis(60),     // Convert 60 minutes to milliseconds
            (int) TimeUnit.MINUTES.toMillis(1440),   // Convert 1440 minutes to milliseconds
            (int) TimeUnit.MINUTES.toMillis(10080)   // Convert 10080 minutes to milliseconds
        };
        assertArrayEquals(
            "nntopReportingPeriodsMs should match the expected millisecond values.",
            expectedValues,
            topConf.nntopReportingPeriodsMs
        );

        // Step 4: Validate the original configuration values retrieved from Configuration object
        String[] configuredPeriodsStr = conf.getTrimmedStrings(DFSConfigKeys.NNTOP_WINDOWS_MINUTES_KEY);
        assertArrayEquals(
            "The configuration values should match the input values.",
            new String[]{"60", "1440", "10080"},
            configuredPeriodsStr
        );
    }
}