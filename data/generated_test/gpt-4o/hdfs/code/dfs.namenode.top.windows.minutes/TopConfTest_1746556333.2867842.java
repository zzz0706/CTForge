package org.apache.hadoop.hdfs.server.namenode.top;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.namenode.top.TopConf;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class TopConfTest {

    private Configuration mockConfiguration;

    @Before
    public void setUp() {
        mockConfiguration = new Configuration();
    }

    @Test
    public void testEmptyPeriodList() {
        // Prerequisite: Setting DFSConfigKeys.NNTOP_WINDOWS_MINUTES_KEY to an empty string
        mockConfiguration.set(DFSConfigKeys.NNTOP_WINDOWS_MINUTES_KEY, "");

        // Step: Initialize a TopConf instance
        TopConf topConf = new TopConf(mockConfiguration);

        // Expected Result: Verify nntopReportingPeriodsMs array is empty
        assertEquals(0, topConf.nntopReportingPeriodsMs.length);
    }
}