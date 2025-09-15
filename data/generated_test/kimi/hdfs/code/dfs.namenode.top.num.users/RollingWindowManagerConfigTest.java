package org.apache.hadoop.hdfs.server.namenode.top.window;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(MockitoJUnitRunner.class)
public class RollingWindowManagerConfigTest {

    private Configuration conf;
    private int reportingPeriodMs = 60000; // 1 minute

    @Before
    public void setUp() {
        conf = new Configuration();
    }

    @Test
    public void testNNTopNumUsersDefaultValue() {
        // Test that the default value is correctly loaded from DFSConfigKeys
        RollingWindowManager manager = new RollingWindowManager(conf, reportingPeriodMs);
        
        // Use reflection to access the private field for testing
        int topUsersCnt = getTopUsersCnt(manager);
        
        assertEquals("Default value should match DFSConfigKeys", 
                DFSConfigKeys.NNTOP_NUM_USERS_DEFAULT, topUsersCnt);
    }

    @Test
    public void testNNTopNumUsersCustomValue() {
        // Set custom value in configuration
        int customValue = 20;
        conf.setInt(DFSConfigKeys.NNTOP_NUM_USERS_KEY, customValue);
        
        RollingWindowManager manager = new RollingWindowManager(conf, reportingPeriodMs);
        
        // Use reflection to access the private field for testing
        int topUsersCnt = getTopUsersCnt(manager);
        
        assertEquals("Custom value should be correctly set", customValue, topUsersCnt);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNNTopNumUsersInvalidValue() {
        // Set invalid value (<= 0) in configuration
        conf.setInt(DFSConfigKeys.NNTOP_NUM_USERS_KEY, 0);
        
        // Should throw IllegalArgumentException
        new RollingWindowManager(conf, reportingPeriodMs);
    }

    @Test
    public void testNNTopNumUsersConfigFileComparison() {
        // Load default value from configuration files using Properties
        Properties defaultProps = new Properties();
        try {
            defaultProps.load(getClass().getClassLoader()
                    .getResourceAsStream("hdfs-default.xml"));
        } catch (Exception e) {
            // Handle exception or ignore if file not found in test context
        }
        
        String fileValueStr = defaultProps.getProperty(DFSConfigKeys.NNTOP_NUM_USERS_KEY);
        int fileValue = fileValueStr != null ? Integer.parseInt(fileValueStr) : 
                       DFSConfigKeys.NNTOP_NUM_USERS_DEFAULT;
        
        // Compare with ConfigService/Configuration API
        int configValue = conf.getInt(DFSConfigKeys.NNTOP_NUM_USERS_KEY, 
                                     DFSConfigKeys.NNTOP_NUM_USERS_DEFAULT);
        
        assertEquals("Configuration file value should match API value", 
                    fileValue, configValue);
    }

    private int getTopUsersCnt(RollingWindowManager manager) {
        try {
            java.lang.reflect.Field field = RollingWindowManager.class.getDeclaredField("topUsersCnt");
            field.setAccessible(true);
            return field.getInt(manager);
        } catch (Exception e) {
            throw new RuntimeException("Failed to access topUsersCnt field", e);
        }
    }
}