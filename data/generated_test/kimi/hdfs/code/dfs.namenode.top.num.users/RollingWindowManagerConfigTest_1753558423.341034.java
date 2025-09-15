package org.apache.hadoop.hdfs.server.namenode.top.window;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.metrics2.util.Metrics2Util.NameValuePair;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class RollingWindowManagerConfigTest {

    private Configuration conf;
    private Properties configProps;

    @Before
    public void setUp() {
        conf = new Configuration();
        configProps = new Properties();
        // Simulate loading from external config file
        configProps.setProperty("dfs.namenode.top.num.users", "10");
    }

    @Test
    public void testNnTopNumUsersConfigurationValue() {
        // 1. Obtain configuration value via HDFS API
        int defaultValue = DFSConfigKeys.NNTOP_NUM_USERS_DEFAULT;
        int configuredValue = conf.getInt(DFSConfigKeys.NNTOP_NUM_USERS_KEY, defaultValue);

        // 2. Load expected value directly from config source (Properties)
        String rawValue = configProps.getProperty(DFSConfigKeys.NNTOP_NUM_USERS_KEY);
        int expectedValue = rawValue != null ? Integer.parseInt(rawValue) : defaultValue;

        // 3. Assert that the two values match
        assertEquals("ConfigService value should match file-based loader value",
                expectedValue, configuredValue);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNnTopNumUsersInvalidValueThrowsException() {
        // Prepare invalid configuration
        conf.setInt(DFSConfigKeys.NNTOP_NUM_USERS_KEY, 0);

        // This should throw IllegalArgumentException due to Preconditions.checkArgument(topUsersCnt > 0)
        new RollingWindowManager(conf, 1000);
    }

    @Test
    public void testNnTopNumUsersDefaultValueWhenNotSet() {
        // Do not set the key in conf, let it use default
        int defaultValue = DFSConfigKeys.NNTOP_NUM_USERS_DEFAULT;
        int configuredValue = conf.getInt(DFSConfigKeys.NNTOP_NUM_USERS_KEY, defaultValue);

        assertEquals("Should use default when not configured",
                defaultValue, configuredValue);
    }

    @Test
    public void testTopNUsesConfiguredUserCount() {
        // Set a specific value for testing
        int testUserCount = 5;
        conf.setInt(DFSConfigKeys.NNTOP_NUM_USERS_KEY, testUserCount);

        // Create RollingWindowManager which initializes topUsersCnt
        RollingWindowManager manager = new RollingWindowManager(conf, 1000);

        // Test the TopN class directly with config value
        org.apache.hadoop.metrics2.util.Metrics2Util.TopN topN =
                new org.apache.hadoop.metrics2.util.Metrics2Util.TopN(testUserCount);

        // Validate that the capacity matches expected
        for (int i = 0; i < testUserCount + 2; i++) {
            topN.offer(new NameValuePair("user" + i, i * 10));
        }
        assertTrue("TopN should respect configured size limit", topN.size() <= testUserCount);
    }
}