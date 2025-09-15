package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.server.namenode.Namesystem;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
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
    public void testHostsProviderClassname_DefaultValue() throws IOException {
        // Ensure the configuration key is not explicitly set to test default behavior
        conf.unset(DFSConfigKeys.DFS_NAMENODE_HOSTS_PROVIDER_CLASSNAME_KEY);

        // Instantiate DatanodeManager which will load the config
        DatanodeManager datanodeManager = new DatanodeManager(blockManager, namesystem, conf);

        // Verify that the default HostFileManager class is used
        HostConfigManager hostConfigManager = datanodeManager.getHostConfigManager();
        assertSame(HostFileManager.class, hostConfigManager.getClass());
    }

    @Test
    public void testHostsProviderClassname_CustomValue() throws IOException {
        // Set custom HostConfigManager implementation class
        conf.set(DFSConfigKeys.DFS_NAMENODE_HOSTS_PROVIDER_CLASSNAME_KEY, MockHostConfigManager.class.getName());

        // Instantiate DatanodeManager which will load the config
        DatanodeManager datanodeManager = new DatanodeManager(blockManager, namesystem, conf);

        // Verify that the custom MockHostConfigManager class is used
        HostConfigManager hostConfigManager = datanodeManager.getHostConfigManager();
        assertSame(MockHostConfigManager.class, hostConfigManager.getClass());
    }

    @Test
    public void testHostsProviderClassname_ReflectionInstantiation() throws IOException {
        // Set the configuration to use HostFileManager explicitly
        conf.set(DFSConfigKeys.DFS_NAMENODE_HOSTS_PROVIDER_CLASSNAME_KEY, HostFileManager.class.getName());

        // Manually instantiate via ReflectionUtils to verify behavior
        Class<? extends HostConfigManager> klass = conf.getClass(
                DFSConfigKeys.DFS_NAMENODE_HOSTS_PROVIDER_CLASSNAME_KEY,
                HostFileManager.class,
                HostConfigManager.class
        );
        HostConfigManager manager = ReflectionUtils.newInstance(klass, conf);

        // Verify correct class instantiation
        assertSame(HostFileManager.class, manager.getClass());
    }

    @Test
    public void testHostsProviderClassname_ConfigValueMatchesFile() {
        // Load expected value directly from configuration (simulating file-based config)
        String expectedClassName = conf.get(DFSConfigKeys.DFS_NAMENODE_HOSTS_PROVIDER_CLASSNAME_KEY, HostFileManager.class.getName());

        // Verify that the configuration service returns the same value as expected
        assertEquals(expectedClassName, HostFileManager.class.getName());
    }

    // Mock HostConfigManager for testing custom class loading
    public static class MockHostConfigManager extends HostConfigManager {
        private Configuration conf;

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
        public String getUpgradeDomain(DatanodeID node) {
            return ""; // No upgrade domain for testing
        }
        
        @Override
        public Iterable<InetSocketAddress> getExcludes() {
            return Collections.emptyList(); // Return empty iterable for testing
        }
        
        @Override
        public Iterable<InetSocketAddress> getIncludes() {
            return Collections.emptyList(); // Return empty iterable for testing
        }

        @Override
        public Configuration getConf() {
            return this.conf;
        }

        @Override
        public void setConf(Configuration conf) {
            this.conf = conf;
        }
    }
}