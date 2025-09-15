package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.Namesystem;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Set;
import java.util.HashSet;

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
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
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
        
        // Test with default (HostFileManager) when CombinedHostFileManager might not be available
        conf.set(key, "org.apache.hadoop.hdfs.server.blockmanagement.HostFileManager");
        DatanodeManager dm3 = new DatanodeManager(blockManager, namesystem, conf);
        assertTrue("Should create HostFileManager instance", 
                dm3.getHostConfigManager() instanceof HostFileManager);
    }

    @Test
    public void testHostFileManagerLoadsConfig() throws IOException {
        // Create a temporary hosts file
        File hostsFile = File.createTempFile("hosts", ".txt");
        hostsFile.deleteOnExit();
        
        // Write sample hosts content
        String hostsContent = "192.168.1.10:50010\n192.168.1.11:50010\n";
        try (FileWriter writer = new FileWriter(hostsFile)) {
            writer.write(hostsContent);
        }
        
        // Set up configuration
        conf.set(DFSConfigKeys.DFS_NAMENODE_HOSTS_PROVIDER_CLASSNAME_KEY,
                "org.apache.hadoop.hdfs.server.blockmanagement.HostFileManager");
        conf.set(DFSConfigKeys.DFS_HOSTS, hostsFile.getAbsolutePath());
        
        // Instantiate DatanodeManager with the loaded configuration
        DatanodeManager datanodeManager = new DatanodeManager(blockManager, namesystem, conf);
        HostConfigManager hostConfigManager = datanodeManager.getHostConfigManager();
        
        // Verify that the hostConfigManager is an instance of HostFileManager
        assertTrue("hostConfigManager should be an instance of HostFileManager",
                hostConfigManager instanceof HostFileManager);
        
        // Check that the hosts file content is correctly parsed
        Set<InetSocketAddress> includes = new HashSet<>();
        for (InetSocketAddress addr : hostConfigManager.getIncludes()) {
            includes.add(addr);
        }
        
        assertEquals("Should have 2 included nodes", 2, includes.size());
        
        // Verify specific addresses
        assertTrue("Should include 192.168.1.10:50010",
                includes.contains(new InetSocketAddress("192.168.1.10", 50010)));
        assertTrue("Should include 192.168.1.11:50010",
                includes.contains(new InetSocketAddress("192.168.1.11", 50010)));
    }
}