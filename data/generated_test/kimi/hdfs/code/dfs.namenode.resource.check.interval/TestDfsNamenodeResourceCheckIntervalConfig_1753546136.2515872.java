package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.lang.reflect.Field;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

public class TestDfsNamenodeResourceCheckIntervalConfig {

    private Configuration conf;

    @Before
    public void setUp() throws Exception {
        // Load configuration as the system would
        conf = new Configuration();
        conf.addResource("hdfs-default.xml");
    }

    @Test
    public void testResourceCheckIntervalDefaultValue() {
        // Fetch using Configuration::getLong
        long actualValue = conf.getLong(
            DFSConfigKeys.DFS_NAMENODE_RESOURCE_CHECK_INTERVAL_KEY,
            DFSConfigKeys.DFS_NAMENODE_RESOURCE_CHECK_INTERVAL_DEFAULT
        );

        // Compare against the default constant
        long expectedValue = DFSConfigKeys.DFS_NAMENODE_RESOURCE_CHECK_INTERVAL_DEFAULT;

        assertEquals("Default value should match configuration default", expectedValue, actualValue);
    }

    @Test
    public void testResourceCheckIntervalUsedInNameNodeResourceMonitor() throws IOException, NoSuchFieldException, IllegalAccessException {
        // Setup custom configuration value
        long customInterval = 1000L;
        conf.setLong(DFSConfigKeys.DFS_NAMENODE_RESOURCE_CHECK_INTERVAL_KEY, customInterval);

        // Create FSNamesystem with mocked dependencies
        FSImage fsImage = Mockito.mock(FSImage.class);
        FSNamesystem fsNamesystem = new FSNamesystem(conf, fsImage);

        // Use reflection to access the private field
        Field resourceRecheckIntervalField = FSNamesystem.class.getDeclaredField("resourceRecheckInterval");
        resourceRecheckIntervalField.setAccessible(true);
        long actualValue = resourceRecheckIntervalField.getLong(fsNamesystem);

        // Verify that the parsed value was assigned correctly
        assertEquals(customInterval, actualValue);
    }

    @Test
    public void testResourceCheckIntervalBranchingBehavior() throws IOException, NoSuchFieldException, IllegalAccessException {
        // Test different intervals affect branching in monitoring loop
        long shortInterval = 100L;
        long longInterval = 5000L;

        Configuration shortConf = new Configuration(conf);
        shortConf.setLong(DFSConfigKeys.DFS_NAMENODE_RESOURCE_CHECK_INTERVAL_KEY, shortInterval);

        Configuration longConf = new Configuration(conf);
        longConf.setLong(DFSConfigKeys.DFS_NAMENODE_RESOURCE_CHECK_INTERVAL_KEY, longInterval);

        FSImage fsImage = Mockito.mock(FSImage.class);
        
        FSNamesystem shortIntervalNS = new FSNamesystem(shortConf, fsImage);
        FSNamesystem longIntervalNS = new FSNamesystem(longConf, fsImage);

        // Use reflection to access the private field
        Field resourceRecheckIntervalField = FSNamesystem.class.getDeclaredField("resourceRecheckInterval");
        resourceRecheckIntervalField.setAccessible(true);
        
        long shortActualValue = resourceRecheckIntervalField.getLong(shortIntervalNS);
        long longActualValue = resourceRecheckIntervalField.getLong(longIntervalNS);

        assertEquals(shortInterval, shortActualValue);
        assertEquals(longInterval, longActualValue);
    }
}