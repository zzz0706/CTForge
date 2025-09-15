package org.apache.hadoop.hdfs.server.namenode.top;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Test;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestTopConf {

    @Test
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testInvalidConfigurationWithZeroMinutes() {
        // Step 1: Prepare the test conditions.
        // Create a Configuration object and set the dfs.namenode.top.windows.minutes property using the HDFS API keys.
        Configuration conf = new Configuration();
        conf.set(DFSConfigKeys.NNTOP_WINDOWS_MINUTES_KEY, "0,5,25");

        // Step 2: Test code.
        // Instantiate the TopConf class and expect an IllegalArgumentException to be thrown.
        try {
            new TopConf(conf);
            // If no exception is thrown during instantiation, the test must fail.
            fail("Expected IllegalArgumentException was not thrown.");
        } catch (IllegalArgumentException e) {
            // Step 3: Code after testing.
            // Validate that the exception's message contains the expected text.
            assertTrue(e.getMessage().contains("minimum reporting period is 1 min"));
        }
    }
}