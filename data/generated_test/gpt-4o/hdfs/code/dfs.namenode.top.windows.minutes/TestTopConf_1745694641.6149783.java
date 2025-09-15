package org.apache.hadoop.hdfs.server.namenode.top;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class TestTopConf {
    // Get configuration value using API
    // Prepare the input conditions for unit testing.

    @Test
    public void testTopConfDefaultEnabledConfig() {
        // Step 1: Create a Configuration object without setting 'dfs.namenode.top.enabled'.
        Configuration conf = new Configuration();

        // Step 2: Pass the Configuration object to the TopConf constructor.
        TopConf topConf = new TopConf(conf);

        // Step 3: Verify that the isEnabled field matches DFSConfigKeys.NNTOP_ENABLED_DEFAULT.
        assertEquals(DFSConfigKeys.NNTOP_ENABLED_DEFAULT, topConf.isEnabled);
    }

    @Test
    public void testTopConfDefaultWindowsMinutesConfig() {
        // Step 1: Create a Configuration object without setting 'dfs.namenode.top.windows.minutes'.
        Configuration conf = new Configuration();

        // Step 2: Instantiate the TopConf class with the configuration.
        TopConf topConf = new TopConf(conf);

        // Step 3: Determine the default expected result.
        String[] defaultPeriods = DFSConfigKeys.NNTOP_WINDOWS_MINUTES_DEFAULT;
        int[] expectedPeriodsMs = new int[defaultPeriods.length];
        for (int i = 0; i < defaultPeriods.length; i++) {
            expectedPeriodsMs[i] = (int) TimeUnit.MINUTES.toMillis(Integer.parseInt(defaultPeriods[i]));
        }

        // Step 4: Verify that nntopReportingPeriodsMs matches the default converted values.
        assertArrayEquals(expectedPeriodsMs, topConf.nntopReportingPeriodsMs);
    }

    @Test
    public void testTopConfCustomWindowsMinutesConfig() {
        // Step 1: Create a Configuration object and set custom values for 'dfs.namenode.top.windows.minutes'.
        Configuration conf = new Configuration();
        conf.setStrings(DFSConfigKeys.NNTOP_WINDOWS_MINUTES_KEY, "5", "10", "15");

        // Step 2: Instantiate the TopConf class with the configuration.
        TopConf topConf = new TopConf(conf);

        // Step 3: Determine the expected result after conversion.
        int[] expectedPeriodsMs = {
                (int) TimeUnit.MINUTES.toMillis(5),
                (int) TimeUnit.MINUTES.toMillis(10),
                (int) TimeUnit.MINUTES.toMillis(15)
        };

        // Step 4: Verify that nntopReportingPeriodsMs matches the expected values.
        assertArrayEquals(expectedPeriodsMs, topConf.nntopReportingPeriodsMs);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testTopConfInvalidWindowsMinutesConfig() {
        // Step 1: Create a Configuration object and set invalid values for 'dfs.namenode.top.windows.minutes'.
        Configuration conf = new Configuration();
        conf.setStrings(DFSConfigKeys.NNTOP_WINDOWS_MINUTES_KEY, "0"); // Invalid: less than 1 minute.

        // Step 2: Instantiate the TopConf class. This should throw an exception.
        new TopConf(conf);
    }
}