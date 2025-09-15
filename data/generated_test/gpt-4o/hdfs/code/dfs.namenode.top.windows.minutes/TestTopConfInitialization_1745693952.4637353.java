package org.apache.hadoop.hdfs.server.namenode.top;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Test;
import static org.junit.Assert.*;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import java.util.concurrent.TimeUnit; // Fix missing import for TimeUnit

public class TestTopConfInitialization {

    // Test the initialization of TopConf with a valid configuration value and verify configuration usage.
    @Test
    public void testTopConfInitializationValidConfig() {
        // Step 1: Create a Configuration instance and set the 'dfs.namenode.top.windows.minutes' property.
        Configuration conf = new Configuration();
        conf.set(DFSConfigKeys.NNTOP_WINDOWS_MINUTES_KEY, "1,5,25");

        // Step 2: Create a TopConf object using the Configuration instance.
        TopConf topConf = new TopConf(conf);

        // Step 3: Verify that isEnabled field matches the default value from DFSConfigKeys.NNTOP_ENABLED_DEFAULT.
        boolean expectedIsEnabled = conf.getBoolean(DFSConfigKeys.NNTOP_ENABLED_KEY, DFSConfigKeys.NNTOP_ENABLED_DEFAULT);
        assertEquals(expectedIsEnabled, topConf.isEnabled);

        // Step 4: Validate that nntopReportingPeriodsMs array contains the correctly converted millisecond values.
        String[] periodsStr = conf.getTrimmedStrings(DFSConfigKeys.NNTOP_WINDOWS_MINUTES_KEY, DFSConfigKeys.NNTOP_WINDOWS_MINUTES_DEFAULT);
        int[] expectedPeriodsMs = new int[periodsStr.length];
        for (int i = 0; i < periodsStr.length; i++) {
            expectedPeriodsMs[i] = Ints.checkedCast(TimeUnit.MINUTES.toMillis(Integer.parseInt(periodsStr[i])));
        }
        assertArrayEquals(expectedPeriodsMs, topConf.nntopReportingPeriodsMs);

        // Step 5: Ensure each reporting period is at least 1 minute.
        for (int periodMs : topConf.nntopReportingPeriodsMs) {
            Preconditions.checkArgument(periodMs >= TimeUnit.MINUTES.toMillis(1), "minimum reporting period is 1 min!");
        }
    }
}