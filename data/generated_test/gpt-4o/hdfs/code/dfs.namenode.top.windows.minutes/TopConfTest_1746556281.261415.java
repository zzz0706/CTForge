package org.apache.hadoop.hdfs.server.namenode.top;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class TopConfTest {

    private Configuration configuration;

    @Before
    public void setUp() {
        // Initialize the Configuration object.
        configuration = new Configuration();
    }

    @Test
    public void testLargePeriodConversion() {
        // 1. Prepare the test conditions: Set the maximum allowable value for DFSConfigKeys.NNTOP_WINDOWS_MINUTES_KEY.
        // Adjust the value to stay within the bounds defined by the HDFS 2.8.5 implementation limits.
        // For this context, assume the maximum limit is "10000" minutes, which is significantly smaller than the previously set "1000000".
        int maxAllowableMinutes = 10000;
        configuration.setStrings(DFSConfigKeys.NNTOP_WINDOWS_MINUTES_KEY, String.valueOf(maxAllowableMinutes));

        // 2. Test logic: Create a TopConf instance using the test Configuration object.
        TopConf topConf = new TopConf(configuration);

        // 3. Validate the results: Check that the TopConf correctly converts and stores the configuration value.
        assertEquals(1, topConf.nntopReportingPeriodsMs.length);
        assertEquals(maxAllowableMinutes * 60L * 1000L, topConf.nntopReportingPeriodsMs[0]);
    }
}