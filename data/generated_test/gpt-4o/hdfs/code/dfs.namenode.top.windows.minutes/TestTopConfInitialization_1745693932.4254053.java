package org.apache.hadoop.hdfs.server.namenode.top;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Test;
import static org.junit.Assert.*;
import java.util.concurrent.TimeUnit;

public class TestTopConfInitialization {
    
    // Validate TopConf initialization with a valid Configuration.
    @Test
    public void testTopConfInitializationValidConfig() {
        // Create a Configuration instance and set 'dfs.namenode.top.windows.minutes' property.
        Configuration conf = new Configuration();
        conf.set(DFSConfigKeys.NNTOP_WINDOWS_MINUTES_KEY, "1,5,25");

        // Create TopConf object using the Configuration instance.
        TopConf topConf = new TopConf(conf);

        // Assert that the isEnabled field matches the default value from DFSConfigKeys.NNTOP_ENABLED_DEFAULT.
        boolean expectedIsEnabled = conf.getBoolean(DFSConfigKeys.NNTOP_ENABLED_KEY, DFSConfigKeys.NNTOP_ENABLED_DEFAULT);
        assertEquals(expectedIsEnabled, topConf.isEnabled);

        // Verify that the nntopReportingPeriodsMs array contains the correctly converted millisecond values.
        String[] periodsStr = conf.getTrimmedStrings(DFSConfigKeys.NNTOP_WINDOWS_MINUTES_KEY, DFSConfigKeys.NNTOP_WINDOWS_MINUTES_DEFAULT);
        int[] expectedPeriodsMs = new int[periodsStr.length];
        for (int i = 0; i < periodsStr.length; i++) {
            expectedPeriodsMs[i] = (int) TimeUnit.MINUTES.toMillis(Integer.parseInt(periodsStr[i])); // Convert from minutes to milliseconds.
        }
        assertArrayEquals(expectedPeriodsMs, topConf.nntopReportingPeriodsMs);
    }
}