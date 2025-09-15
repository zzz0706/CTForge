package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Method;

import static org.junit.Assert.*;

public class ImageServletConfigTest {

    @Test
    public void testBandwidthThrottlerDisabledForBootstrapStandby() throws Exception {
        // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        // 2. Prepare the test conditions.
        Configuration conf = new Configuration();
        conf.setLong(DFSConfigKeys.DFS_IMAGE_TRANSFER_BOOTSTRAP_STANDBY_RATE_KEY, 0L);
        
        // 3. Test code.
        // Use reflection to access the private method
        Method method = ImageServlet.class.getDeclaredMethod("getThrottlerForBootstrapStandby", Configuration.class);
        method.setAccessible(true);
        Object throttler = method.invoke(null, conf);
        
        // 4. Code after testing.
        assertNull("Throttler should be null when bandwidth is set to 0", throttler);
    }
    
    @Test
    public void testBandwidthThrottlerEnabledForBootstrapStandby() throws Exception {
        // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        // 2. Prepare the test conditions.
        Configuration conf = new Configuration();
        long bandwidth = 1024L; // 1KB/s
        conf.setLong(DFSConfigKeys.DFS_IMAGE_TRANSFER_BOOTSTRAP_STANDBY_RATE_KEY, bandwidth);
        
        // 3. Test code.
        // Use reflection to access the private method
        Method method = ImageServlet.class.getDeclaredMethod("getThrottlerForBootstrapStandby", Configuration.class);
        method.setAccessible(true);
        Object throttler = method.invoke(null, conf);
        
        // 4. Code after testing.
        assertNotNull("Throttler should not be null when bandwidth is greater than 0", throttler);
    }
    
    @Test
    public void testBootstrapStandbyBandwidthConfigDefaultValue() throws IOException {
        // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        // 2. Prepare the test conditions.
        long expectedDefault = DFSConfigKeys.DFS_IMAGE_TRANSFER_BOOTSTRAP_STANDBY_RATE_DEFAULT;

        // 3. Test code.
        Configuration conf = new Configuration();
        long actualValue = conf.getLong(
                DFSConfigKeys.DFS_IMAGE_TRANSFER_BOOTSTRAP_STANDBY_RATE_KEY,
                DFSConfigKeys.DFS_IMAGE_TRANSFER_BOOTSTRAP_STANDBY_RATE_DEFAULT);

        // 4. Code after testing.
        assertEquals("Default value should match DFSConfigKeys constant", expectedDefault, actualValue);
    }

    @Test
    public void testConfigurationFileMatchesRuntime() throws IOException {
        // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        // 2. Prepare the test conditions.
        // Create a fresh configuration to load defaults
        Configuration conf = new Configuration();
        
        // 3. Test code.
        // Get value via Configuration API
        long runtimeValue = conf.getLong(
                DFSConfigKeys.DFS_IMAGE_TRANSFER_BOOTSTRAP_STANDBY_RATE_KEY,
                DFSConfigKeys.DFS_IMAGE_TRANSFER_BOOTSTRAP_STANDBY_RATE_DEFAULT);
        
        // 4. Code after testing.
        // Verify it matches the expected default
        assertEquals("Runtime config value should match DFSConfigKeys default", 
                DFSConfigKeys.DFS_IMAGE_TRANSFER_BOOTSTRAP_STANDBY_RATE_DEFAULT,
                runtimeValue);
    }
}