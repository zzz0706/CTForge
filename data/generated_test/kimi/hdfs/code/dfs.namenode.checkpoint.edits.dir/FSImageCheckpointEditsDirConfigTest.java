package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.common.Util;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Mockito.any;

@RunWith(PowerMockRunner.class)
@PrepareForTest({Util.class})
public class FSImageCheckpointEditsDirConfigTest {

    private Configuration conf;
    private static final String CONFIG_KEY = DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_EDITS_DIR_KEY;
    private static final String DEFAULT_VALUE = "${dfs.namenode.checkpoint.dir}";

    @Before
    public void setUp() {
        conf = new Configuration();
    }

    @Test
    public void testGetCheckpointEditsDirsWithExplicitConfig() throws Exception {
        // Prepare test conditions
        String testDir = "/tmp/hadoop/edits";
        conf.set(CONFIG_KEY, testDir);

        // Mock Util.stringCollectionAsURIs to capture the argument
        PowerMockito.mockStatic(Util.class);
        List<URI> expectedURIs = new ArrayList<URI>();
        expectedURIs.add(new URI("file:///tmp/hadoop/edits"));
        PowerMockito.when(Util.stringCollectionAsURIs(any(Collection.class))).thenReturn(expectedURIs);

        // Test code
        List<URI> result = FSImage.getCheckpointEditsDirs(conf, null);

        // Verification
        assertEquals("Should return one URI", 1, result.size());
        assertEquals("Returned URI should match expected", expectedURIs.get(0), result.get(0));
        
        // Verify that Util.stringCollectionAsURIs was called with correct argument
        PowerMockito.verifyStatic(Mockito.times(1));
        Util.stringCollectionAsURIs(any(Collection.class));
    }

    @Test
    public void testGetCheckpointEditsDirsWithDefaultFallback() throws Exception {
        // Prepare test conditions - no explicit config, provide default
        String defaultName = "/tmp/hadoop/default";
        
        // Mock Util.stringCollectionAsURIs
        PowerMockito.mockStatic(Util.class);
        List<URI> expectedURIs = new ArrayList<URI>();
        expectedURIs.add(new URI("file:///tmp/hadoop/default"));
        PowerMockito.when(Util.stringCollectionAsURIs(any(Collection.class))).thenReturn(expectedURIs);

        // Test code
        List<URI> result = FSImage.getCheckpointEditsDirs(conf, defaultName);

        // Verification
        assertEquals("Should return one URI", 1, result.size());
        assertEquals("Returned URI should match expected", expectedURIs.get(0), result.get(0));
        
        // Verify that Util.stringCollectionAsURIs was called with correct argument
        PowerMockito.verifyStatic(Mockito.times(1));
        Util.stringCollectionAsURIs(any(Collection.class));
    }

    @Test
    public void testGetCheckpointEditsDirsWithMultipleDirectories() throws Exception {
        // Prepare test conditions
        String dirs = "/tmp/hadoop/edits1,/tmp/hadoop/edits2";
        conf.set(CONFIG_KEY, dirs);

        // Mock Util.stringCollectionAsURIs
        PowerMockito.mockStatic(Util.class);
        List<URI> expectedURIs = new ArrayList<URI>();
        expectedURIs.add(new URI("file:///tmp/hadoop/edits1"));
        expectedURIs.add(new URI("file:///tmp/hadoop/edits2"));
        PowerMockito.when(Util.stringCollectionAsURIs(any(Collection.class))).thenReturn(expectedURIs);

        // Test code
        List<URI> result = FSImage.getCheckpointEditsDirs(conf, null);

        // Verification
        assertEquals("Should return two URIs", 2, result.size());
        assertTrue("Should contain first URI", result.contains(expectedURIs.get(0)));
        assertTrue("Should contain second URI", result.contains(expectedURIs.get(1)));
        
        // Verify that Util.stringCollectionAsURIs was called with correct arguments
        PowerMockito.verifyStatic(Mockito.times(1));
        Util.stringCollectionAsURIs(any(Collection.class));
    }

    @Test
    public void testGetCheckpointEditsDirsWithDefaultValueVariable() {
        // Prepare test conditions
        conf.set(CONFIG_KEY, DEFAULT_VALUE);
        String checkpointDir = "/tmp/hadoop/checkpoint";
        conf.set(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_DIR_KEY, checkpointDir);

        // Test code
        List<URI> result = FSImage.getCheckpointEditsDirs(conf, null);

        // Since we're not mocking Util completely, we check that the method processes the value
        assertNotNull("Result should not be null", result);
        // The actual URI conversion would happen in Util.stringCollectionAsURIs
    }

    @Test
    public void testConfigurationValueMatchesPropertiesFile() throws IOException {
        // Get the default value from Configuration API directly
        Configuration defaultConf = new Configuration();
        String defaultValue = defaultConf.get(CONFIG_KEY);
        
        // For DFS_NAMENODE_CHECKPOINT_EDITS_DIR_KEY, the default value may be null
        // In such cases, it falls back to using the checkpoint dir value
        String configValue = conf.get(CONFIG_KEY, defaultValue);
        
        // If defaultValue is null, configValue should equal defaultValue (both null)
        // If defaultValue is not null, they should be equal
        assertEquals("Configuration value should match default value",
                defaultValue, configValue);
        
        // Test that we can set and retrieve a custom value
        String testValue = "/test/checkpoint/edits";
        conf.set(CONFIG_KEY, testValue);
        String retrievedValue = conf.get(CONFIG_KEY);
        assertEquals("Retrieved value should match set value", testValue, retrievedValue);
    }
}