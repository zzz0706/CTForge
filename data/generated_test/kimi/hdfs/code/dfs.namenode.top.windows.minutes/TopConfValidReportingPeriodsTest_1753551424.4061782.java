package org.apache.hadoop.hdfs.server.namenode.top;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class TopConfValidReportingPeriodsTest {

    private Configuration conf;

    @Before
    public void setUp() {
        conf = new Configuration();
    }

    @Test
    // Test that valid reporting periods from configuration are correctly parsed and converted to milliseconds.
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions by setting dfs.namenode.top.windows.minutes to "1,5,25".
    // 3. Instantiate TopConf with the Configuration object and verify the nntopReportingPeriodsMs array.
    // 4. Check that the array contains values equivalent to 1, 5, and 25 minutes in milliseconds.
    public void testValidReportingPeriodsParsedCorrectly() {
        // Step 2: Prepare test conditions
        String testValue = "1,5,25";
        conf.set(DFSConfigKeys.NNTOP_WINDOWS_MINUTES_KEY, testValue);

        // Step 3: Instantiate TopConf with the Configuration object
        TopConf topConf = new TopConf(conf);

        // Step 4: Verify that the nntopReportingPeriodsMs array contains correct millisecond values
        assertEquals("Should have 3 reporting periods", 3, topConf.nntopReportingPeriodsMs.length);
        
        long expected1Minute = TimeUnit.MINUTES.toMillis(1);
        long expected5Minutes = TimeUnit.MINUTES.toMillis(5);
        long expected25Minutes = TimeUnit.MINUTES.toMillis(25);
        
        assertEquals("First period should be 1 minute in milliseconds", expected1Minute, topConf.nntopReportingPeriodsMs[0]);
        assertEquals("Second period should be 5 minutes in milliseconds", expected5Minutes, topConf.nntopReportingPeriodsMs[1]);
        assertEquals("Third period should be 25 minutes in milliseconds", expected25Minutes, topConf.nntopReportingPeriodsMs[2]);
    }
}