package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.Namesystem;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class DatanodeManagerHostsProviderConfigTest {

    private Configuration conf;

    @Mock
    private BlockManager blockManager;

    @Mock
    private Namesystem namesystem;

    @Before
    public void setUp() throws IOException {
        MockitoAnnotations.initMocks(this);
        conf = new Configuration();
    }

    @Test
    public void testHostsProviderClassnameConfigurationDefaultValue() throws IOException {
        // Prepare test conditions - use default configuration
        String key = DFSConfigKeys.DFS_NAMENODE_HOSTS_PROVIDER_CLASSNAME_KEY;
        
        // Get value via ConfigService (in this case Hadoop Configuration)
        String configValue = conf.get(key, "org.apache.hadoop.hdfs.server.blockmanagement.HostFileManager");
        
        // Test code - verify that DatanodeManager uses the correct default class
        DatanodeManager datanodeManager = new DatanodeManager(blockManager, namesystem, conf);
        HostConfigManager hostConfigManager = datanodeManager.getHostConfigManager();
        
        // Verify that the default HostFileManager class is instantiated
        assertNotNull("HostConfigManager should not be null", hostConfigManager);
        assertEquals("Default HostFileManager should be used", 
                "org.apache.hadoop.hdfs.server.blockmanagement.HostFileManager", 
                hostConfigManager.getClass().getName());
    }

    @Test
    public void testHostsProviderClassnameConfigurationCustomValue() throws IOException {
        // Prepare test conditions - set custom configuration
        String key = DFSConfigKeys.DFS_NAMENODE_HOSTS_PROVIDER_CLASSNAME_KEY;
        String customValue = "org.apache.hadoop.hdfs.server.blockmanagement.HostFileManager";
        conf.set(key, customValue);
        
        // Get value via ConfigService
        String configValue = conf.get(key);
        
        // Test code - verify that DatanodeManager uses the custom class
        DatanodeManager datanodeManager = new DatanodeManager(blockManager, namesystem, conf);
        HostConfigManager hostConfigManager = datanodeManager.getHostConfigManager();
        
        // Verify that the custom HostFileManager class is instantiated
        assertNotNull("HostConfigManager should not be null", hostConfigManager);
        assertEquals("Custom HostFileManager should be used", 
                "org.apache.hadoop.hdfs.server.blockmanagement.HostFileManager", 
                hostConfigManager.getClass().getName());
    }

    @Test
    public void testHostsProviderClassnameConfigurationWithReflectionUtils() throws IOException {
        // Prepare test conditions
        String key = DFSConfigKeys.DFS_NAMENODE_HOSTS_PROVIDER_CLASSNAME_KEY;
        
        // Get value via ConfigService
        Class<? extends HostConfigManager> clazz = conf.getClass(
                key, HostFileManager.class, HostConfigManager.class);
        
        // Test code - verify ReflectionUtils.newInstance behavior
        HostConfigManager hostConfigManager = ReflectionUtils.newInstance(clazz, conf);
        
        // Verify correct instantiation
        assertNotNull("HostConfigManager should be instantiated", hostConfigManager);
        assertTrue("Should be instance of HostConfigManager", 
                HostConfigManager.class.isAssignableFrom(hostConfigManager.getClass()));
        
        // Verify it's the default implementation
        assertEquals("Should be HostFileManager instance", 
                HostFileManager.class, hostConfigManager.getClass());
    }

    @Test
    public void testHostsProviderClassnameConfigurationBranchingBehavior() throws IOException {
        // Test different configuration values to ensure proper branching
        String key = DFSConfigKeys.DFS_NAMENODE_HOSTS_PROVIDER_CLASSNAME_KEY;
        
        // Test with HostFileManager
        conf.set(key, "org.apache.hadoop.hdfs.server.blockmanagement.HostFileManager");
        DatanodeManager dm1 = new DatanodeManager(blockManager, namesystem, conf);
        assertTrue("Should create HostFileManager instance", 
                dm1.getHostConfigManager() instanceof HostFileManager);
        
        // Test with CombinedHostFileManager (if available in classpath)
        try {
            Class.forName("org.apache.hadoop.hdfs.server.blockmanagement.CombinedHostFileManager");
            conf.set(key, "org.apache.hadoop.hdfs.server.blockmanagement.CombinedHostFileManager");
            DatanodeManager dm2 = new DatanodeManager(blockManager, namesystem, conf);
            assertTrue("Should create CombinedHostFileManager instance", 
                    dm2.getHostConfigManager() instanceof CombinedHostFileManager);
        } catch (ClassNotFoundException e) {
            // CombinedHostFileManager not available, test with HostFileManager again
            conf.set(key, "org.apache.hadoop.hdfs.server.blockmanagement.HostFileManager");
            DatanodeManager dm3 = new DatanodeManager(blockManager, namesystem, conf);
            assertTrue("Should create HostFileManager instance", 
                    dm3.getHostConfigManager() instanceof HostFileManager);
        }
    }
}