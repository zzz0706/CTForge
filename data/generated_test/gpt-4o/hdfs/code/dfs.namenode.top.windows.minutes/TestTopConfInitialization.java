package org.apache.hadoop.hdfs.server.namenode.top;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Test;
import static org.junit.Assert.*;

public class TestTopConfInitialization {
    
    // Prepare the input conditions for unit testing.
    @Test
    public void testTopConfInitializationValidConfig() {
        // Create a Configuration instance and use the API to retrieve configuration values.
        Configuration conf = new Configuration();
        
        // Create TopConf object using the Configuration instance.
        TopConf topConf = new TopConf(conf);
        
        // Assert that the isEnabled field matches the default value from DFSConfigKeys.NNTOP_ENABLED_DEFAULT.
        boolean isEnabledDefault = conf.getBoolean(DFSConfigKeys.NNTOP_ENABLED_KEY, DFSConfigKeys.NNTOP_ENABLED_DEFAULT);
        assertEquals(isEnabledDefault, topConf.isEnabled);
        
        // Verify that the nntopReportingPeriodsMs array contains the correctly converted millisecond values.
        String[] periodsStr = conf.getTrimmedStrings(DFSConfigKeys.NNTOP_WINDOWS_MINUTES_KEY, DFSConfigKeys.NNTOP_WINDOWS_MINUTES_DEFAULT);
        int[] expectedPeriodsMs = new int[periodsStr.length];
        for (int i = 0; i < periodsStr.length; i++) {
            expectedPeriodsMs[i] = (int) (Long.parseLong(periodsStr[i]) * 60 * 1000); // Convert from minutes to milliseconds.
        }
        assertArrayEquals(expectedPeriodsMs, topConf.nntopReportingPeriodsMs);
    }
}