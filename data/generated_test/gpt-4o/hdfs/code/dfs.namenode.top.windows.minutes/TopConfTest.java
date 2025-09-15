package org.apache.hadoop.hdfs.server.namenode.top;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.namenode.top.TopConf;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class TopConfTest {

    private Configuration mockConfiguration;

    @Before
    public void setUp() {
        mockConfiguration = new Configuration();
    }

    @Test
    public void testValidPeriodConversion() {
        // Prerequisites: A Configuration object with a valid positive integer for DFSConfigKeys.NNTOP_WINDOWS_MINUTES_KEY.
        mockConfiguration.set(DFSConfigKeys.NNTOP_WINDOWS_MINUTES_KEY, "5");

        // Steps: Perform testing actions as described.
        TopConf topConf = new TopConf(mockConfiguration);

        // Expected result: Verify the outcome matches expectations.
        assertEquals(1, topConf.nntopReportingPeriodsMs.length);
        assertEquals(300000, topConf.nntopReportingPeriodsMs[0]); // 5 minutes in milliseconds (300000 ms)
    }
}