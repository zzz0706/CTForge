package org.apache.hadoop.hdfs.server.namenode.top;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Test;
import static org.junit.Assert.*;

public class TestTopConf {

    // Get configuration value using API
    // Prepare the input conditions for unit testing.
    @Test
    public void testTopConfInitializationInvalidConfig() {
        // Fetch configuration value using API
        Configuration conf = new Configuration();
        conf.set(DFSConfigKeys.NNTOP_WINDOWS_MINUTES_KEY, "0");

        try {
            // Attempt to create TopConf object
            TopConf topConf = new TopConf(conf);
            fail("Expected IllegalArgumentException was not thrown.");
        } catch (IllegalArgumentException e) {
            // Validate the exception message
            assertEquals("minimum reporting period is 1 min!", e.getMessage());
        }
    }
}