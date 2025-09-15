package org.apache.hadoop.hdfs.server.namenode.top;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Test;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class TestTopConf {
    // Get configuration value using API
    // Prepare the input conditions for unit testing.
    @Test
    public void testTopConfInitializationInvalidConfig() {
        // Create a Configuration object and set 'dfs.namenode.top.windows.minutes' to '0'
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

    // Get configuration value using API
    // Prepare the input conditions for unit testing.
    @Test
    public void testTopConfInitializationValidConfig() {
        // Create a Configuration object and set 'dfs.namenode.top.windows.minutes' to '5'
        Configuration conf = new Configuration();
        conf.set(DFSConfigKeys.NNTOP_WINDOWS_MINUTES_KEY, "5");

        try {
            // Attempt to create TopConf object
            TopConf topConf = new TopConf(conf);
            assertNotNull(topConf);
        } catch (IllegalArgumentException e) {
            fail("Unexpected IllegalArgumentException was thrown: " + e.getMessage());
        }
    }

    // Get configuration value using API
    // Prepare the input conditions for unit testing.
    @Test
    public void testTopConfDefaultValues() {
        // Create a Configuration object without setting 'dfs.namenode.top.windows.minutes'
        Configuration conf = new Configuration();

        try {
            // Attempt to create TopConf object with default values
            TopConf topConf = new TopConf(conf);
            assertNotNull(topConf);
        } catch (IllegalArgumentException e) {
            fail("Unexpected IllegalArgumentException was thrown: " + e.getMessage());
        }
    }

    // Get configuration value using API
    // Prepare the input conditions for unit testing.
    @Test
    public void testTopConfInitializationMultiplePeriodsInvalid() {
        // Create a Configuration object and set 'dfs.namenode.top.windows.minutes' to an invalid value
        Configuration conf = new Configuration();
        conf.set(DFSConfigKeys.NNTOP_WINDOWS_MINUTES_KEY, "10,0,5");

        try {
            // Attempt to create TopConf object
            TopConf topConf = new TopConf(conf);
            fail("Expected IllegalArgumentException was not thrown.");
        } catch (IllegalArgumentException e) {
            // Validate the exception message
            assertEquals("minimum reporting period is 1 min!", e.getMessage());
        }
    }

    // Get configuration value using API
    // Prepare the input conditions for unit testing.
    @Test
    public void testTopConfInitializationMultiplePeriodsValid() {
        // Create a Configuration object and set 'dfs.namenode.top.windows.minutes' to valid values
        Configuration conf = new Configuration();
        conf.set(DFSConfigKeys.NNTOP_WINDOWS_MINUTES_KEY, "10,15,5");

        try {
            // Attempt to create TopConf object
            TopConf topConf = new TopConf(conf);
            assertNotNull(topConf);
        } catch (IllegalArgumentException e) {
            fail("Unexpected IllegalArgumentException was thrown: " + e.getMessage());
        }
    }
}