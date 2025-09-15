package org.apache.hadoop.hdfs.server.namenode.top;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Test;
import static org.junit.Assert.assertArrayEquals;

public class TestTopConf {

    @Test
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testValidConfigurationForTopConf() {
        // Step 1: Create a Configuration object.
        Configuration conf = new Configuration();

        // Step 2: Prepare test conditions by setting a valid configuration value for `dfs.namenode.top.windows.minutes`.
        conf.set(DFSConfigKeys.NNTOP_WINDOWS_MINUTES_KEY, "1,5,25");

        // Step 3: Test the code by instantiating the TopConf class with the configured Configuration object.
        TopConf topConf = new TopConf(conf);

        // Step 4: Verify that the nntopReportingPeriodsMs array is correctly populated.
        // Expected millisecond values for reporting periods of 1, 5, and 25 minutes.
        int[] expectedMilliseconds = {
            (int) java.util.concurrent.TimeUnit.MINUTES.toMillis(1),
            (int) java.util.concurrent.TimeUnit.MINUTES.toMillis(5),
            (int) java.util.concurrent.TimeUnit.MINUTES.toMillis(25)
        };

        assertArrayEquals("The nntopReportingPeriodsMs array is not populated correctly.",
                          expectedMilliseconds, topConf.nntopReportingPeriodsMs);
    }
}