package org.apache.hadoop.hdfs.client;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import static org.junit.Assert.assertTrue;

public class TestDfsClientSlowIoWarningThreshold {

    @Test
    public void testDfsClientSlowIoWarningThresholdConfiguration() {
        // Step 1: Create a Configuration object to read the configuration.
        Configuration conf = new Configuration();

        // Step 2: Fetch the configuration value using the key from the configuration file or system.
        // Default value is 30000 ms, as specified in the source code.
        String configKey = "dfs.client.slow.io.warning.threshold.ms";
        long defaultValue = 30000;  // Default value mentioned in the source code.
        long thresholdValue = conf.getLong(configKey, defaultValue);

        // Step 3: Validate the configuration value.
        // Ensure that the value is non-negative (time in milliseconds should never be negative).
        assertTrue("Configuration value for dfs.client.slow.io.warning.threshold.ms should be non-negative.",
                thresholdValue >= 0);
                
        assertEquals(30000, thresholdValue);
 
    }
}