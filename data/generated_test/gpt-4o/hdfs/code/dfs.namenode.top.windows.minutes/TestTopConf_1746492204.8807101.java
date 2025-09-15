package org.apache.hadoop.hdfs.server.namenode.top;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Test;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

public class TestTopConf {

    @Test
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testValidConfigurationForTopConf() {
        // Step 1: Create a valid Configuration object using the Hadoop API.
        Configuration conf = new Configuration();

        // Step 2: Prepare the test conditions by configuring `dfs.namenode.top.windows.minutes` with valid values.
        conf.set(DFSConfigKeys.NNTOP_WINDOWS_MINUTES_KEY, "1,5,25");

        // Step 3: Instantiate the TopConf class using the configuration object.
        TopConf topConf = new TopConf(conf);

        // Step 4: Verify that the nntopReportingPeriodsMs array in TopConf is correctly populated.
        int[] expectedMilliseconds = {
            (int) java.util.concurrent.TimeUnit.MINUTES.toMillis(1),
            (int) java.util.concurrent.TimeUnit.MINUTES.toMillis(5),
            (int) java.util.concurrent.TimeUnit.MINUTES.toMillis(25)
        };

        // Compare the resulting array from TopConf with the expected values.
        assertArrayEquals(
            "The nntopReportingPeriodsMs array is not correctly populated with converted millisecond values.",
            expectedMilliseconds,
            topConf.nntopReportingPeriodsMs
        );

        // Additional verification: Ensure TopConf is enabled if the default configuration is set to true.
        assertTrue(
            "TopConf should be enabled when the default setting is enabled.",
            topConf.isEnabled
        );
    }
}