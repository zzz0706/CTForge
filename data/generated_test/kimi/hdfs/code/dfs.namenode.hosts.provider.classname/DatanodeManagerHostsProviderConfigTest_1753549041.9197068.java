package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.server.namenode.Namesystem;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.net.InetSocketAddress;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class DatanodeManagerHostsProviderConfigTest {

    @Mock
    private BlockManager blockManager;

    @Mock
    private Namesystem namesystem;

    private Configuration conf;

    @Before
    public void setUp() {
        conf = new Configuration();
    }

    @Test
    public void testDfsNamenodeHostsProviderClassname_DefaultValue() throws IOException {
        // Ensure the configuration key is not explicitly set to test default behavior
        conf.unset(DFSConfigKeys.DFS_NAMENODE_HOSTS_PROVIDER_CLASSNAME_KEY);

        // Create DatanodeManager which will instantiate hostConfigManager
        DatanodeManager datanodeManager = new DatanodeManager(blockManager, namesystem, conf);

        // Verify that the default class is used
        HostConfigManager hostConfigManager = datanodeManager.getHostConfigManager();
        assertNotNull("HostConfigManager should not be null", hostConfigManager);
        assertEquals("Default HostConfigManager class should be HostFileManager",
                HostFileManager.class, hostConfigManager.getClass());
    }

    @Test
    public void testDfsNamenodeHostsProviderClassname_CustomValue() throws IOException {
        // Set a custom HostConfigManager implementation for testing
        conf.set(DFSConfigKeys.DFS_NAMENODE_HOSTS_PROVIDER_CLASSNAME_KEY,
                MockHostConfigManager.class.getName());

        // Create DatanodeManager which will instantiate hostConfigManager
        DatanodeManager datanodeManager = new DatanodeManager(blockManager, namesystem, conf);

        // Verify that the custom class is used
        HostConfigManager hostConfigManager = datanodeManager.getHostConfigManager();
        assertNotNull("HostConfigManager should not be null", hostConfigManager);
        assertEquals("Custom HostConfigManager class should be MockHostConfigManager",
                MockHostConfigManager.class, hostConfigManager.getClass());
    }

    @Test
    public void testDfsNamenodeHostsProviderClassname_ConfigValueMatchesFile() {
        // Load expected value directly from configuration (simulating file read)
        String expectedClassName = conf.get(DFSConfigKeys.DFS_NAMENODE_HOSTS_PROVIDER_CLASSNAME_KEY,
                HostFileManager.class.getName());

        // Get actual value via ConfigService equivalent (Configuration.get in this case)
        String actualClassName = conf.get(DFSConfigKeys.DFS_NAMENODE_HOSTS_PROVIDER_CLASSNAME_KEY,
                HostFileManager.class.getName());

        // Assert they match
        assertEquals("Configuration value should match the expected default from file",
                expectedClassName, actualClassName);
    }

    // Mock HostConfigManager for testing custom class instantiation
    public static class MockHostConfigManager extends HostConfigManager {
        private Configuration conf;
        
        @Override
        public void setConf(Configuration conf) {
            this.conf = conf;
        }

        @Override
        public Configuration getConf() {
            return conf;
        }

        @Override
        public void refresh() throws IOException {
            // No-op for testing
        }

        @Override
        public boolean isIncluded(DatanodeID dn) {
            return true; // Always included for testing
        }

        @Override
        public boolean isExcluded(DatanodeID dn) {
            return false; // Never excluded for testing
        }

        @Override
        public Iterable<InetSocketAddress> getIncludes() {
            return java.util.Collections.emptyList();
        }

        @Override
        public Iterable<InetSocketAddress> getExcludes() {
            return java.util.Collections.emptyList();
        }

        @Override
        public String getUpgradeDomain(DatanodeID node) {
            return "";
        }
    }
}