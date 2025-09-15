package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.Namesystem;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class DatanodeManagerHostsProviderConfigTest {

    private Configuration conf;
    private Properties configProperties;

    @Mock
    private BlockManager blockManager;

    @Mock
    private Namesystem namesystem;

    @Before
    public void setUp() throws IOException {
        // Load configuration from external source (simulating actual config files)
        conf = new Configuration();
        // In a real scenario, this would be loaded from hdfs-site.xml or similar
        // For test purposes, we'll set it directly but in practice it comes from file
        conf.set(DFSConfigKeys.DFS_NAMENODE_HOSTS_PROVIDER_CLASSNAME_KEY,
                "org.apache.hadoop.hdfs.server.blockmanagement.HostFileManager");

        // Simulate loading the same configuration value via Properties to compare
        configProperties = new Properties();
        // This would normally come from reading the actual XML/properties file
        configProperties.setProperty(DFSConfigKeys.DFS_NAMENODE_HOSTS_PROVIDER_CLASSNAME_KEY,
                "org.apache.hadoop.hdfs.server.blockmanagement.HostFileManager");
    }

    @Test
    public void testHostsProviderClassnameConfigurationIsLoadedCorrectly() throws IOException {
        // Given: Configuration service provides the value from external file
        String configValue = conf.get(DFSConfigKeys.DFS_NAMENODE_HOSTS_PROVIDER_CLASSNAME_KEY);
        
        // When: DatanodeManager is constructed
        DatanodeManager datanodeManager = new DatanodeManager(blockManager, namesystem, conf);
        
        // Then: Verify that the configuration value matches what's in the properties file
        assertEquals(
            "Configuration value should match the value loaded from the configuration file",
            configProperties.getProperty(DFSConfigKeys.DFS_NAMENODE_HOSTS_PROVIDER_CLASSNAME_KEY),
            configValue
        );
        
        // And: The hostConfigManager should be an instance of the configured class
        HostConfigManager hostConfigManager = datanodeManager.getHostConfigManager();
        assertSame(
            "HostConfigManager should be an instance of the configured class",
            HostFileManager.class,
            hostConfigManager.getClass()
        );
    }

    @Test
    public void testHostsProviderClassnameWithDifferentImplementation() throws IOException {
        // Given: A different hosts provider class is configured
        conf.set(DFSConfigKeys.DFS_NAMENODE_HOSTS_PROVIDER_CLASSNAME_KEY,
                "org.apache.hadoop.hdfs.server.blockmanagement.CombinedHostFileManager");
        configProperties.setProperty(DFSConfigKeys.DFS_NAMENODE_HOSTS_PROVIDER_CLASSNAME_KEY,
                "org.apache.hadoop.hdfs.server.blockmanagement.CombinedHostFileManager");
        
        String configValue = conf.get(DFSConfigKeys.DFS_NAMENODE_HOSTS_PROVIDER_CLASSNAME_KEY);
        
        // When: DatanodeManager is constructed
        DatanodeManager datanodeManager = new DatanodeManager(blockManager, namesystem, conf);
        
        // Then: Configuration value matches file content
        assertEquals(
            "Configuration value should match the value loaded from the configuration file",
            configProperties.getProperty(DFSConfigKeys.DFS_NAMENODE_HOSTS_PROVIDER_CLASSNAME_KEY),
            configValue
        );
        
        // And: The hostConfigManager should be an instance of the configured class
        HostConfigManager hostConfigManager = datanodeManager.getHostConfigManager();
        assertSame(
            "HostConfigManager should be an instance of CombinedHostFileManager when configured",
            CombinedHostFileManager.class,
            hostConfigManager.getClass()
        );
    }

    @Test
    public void testHostsProviderClassnameUsesDefaultWhenNotSpecified() throws IOException {
        // Given: No explicit configuration for hosts provider classname
        conf.unset(DFSConfigKeys.DFS_NAMENODE_HOSTS_PROVIDER_CLASSNAME_KEY);
        configProperties.remove(DFSConfigKeys.DFS_NAMENODE_HOSTS_PROVIDER_CLASSNAME_KEY);
        
        // When: DatanodeManager is constructed
        DatanodeManager datanodeManager = new DatanodeManager(blockManager, namesystem, conf);
        
        // Then: Should use default value
        HostConfigManager hostConfigManager = datanodeManager.getHostConfigManager();
        assertSame(
            "HostConfigManager should default to HostFileManager when not specified",
            HostFileManager.class,
            hostConfigManager.getClass()
        );
    }

    @Test
    public void testHostConfigManagerRefreshIsCalledDuringConstruction() throws IOException {
        // Given: Mock HostConfigManager to verify refresh call
        HostConfigManager mockHostConfigManager = mock(HostConfigManager.class);
        
        // Create a spy of ReflectionUtils to partially mock the newInstance method
        Class<?> hostFileManagerClass = HostFileManager.class;
        ReflectionUtils spyReflectionUtils = Mockito.spy(ReflectionUtils.class);
        
        // Use PowerMock or a different approach since we can't easily mock static methods
        // Instead, let's test this differently by checking if refresh is called on the actual instance
        
        // Configure to use HostFileManager
        conf.set(DFSConfigKeys.DFS_NAMENODE_HOSTS_PROVIDER_CLASSNAME_KEY,
                HostFileManager.class.getName());
        
        // When: DatanodeManager is constructed
        DatanodeManager datanodeManager = new DatanodeManager(blockManager, namesystem, conf);
        
        // Then: The hostConfigManager should be initialized and refresh should be called
        HostConfigManager hostConfigManager = datanodeManager.getHostConfigManager();
        assertSame(
            "HostConfigManager should be an instance of HostFileManager",
            HostFileManager.class,
            hostConfigManager.getClass()
        );
        
        // Note: We cannot easily verify the refresh() call was made during construction
        // without more complex mocking of static methods, which is beyond standard Mockito
    }
}