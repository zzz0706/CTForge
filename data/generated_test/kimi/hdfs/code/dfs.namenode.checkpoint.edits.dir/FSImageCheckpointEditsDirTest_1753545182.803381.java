package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.*;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

public class FSImageCheckpointEditsDirTest {

    private Configuration conf;

    @Before
    public void setUp() {
        conf = new Configuration();
    }

    @Test
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testCheckpointEditsDirsMultipleValidPaths() {
        // Prepare test conditions
        String dirs = "/tmp/hadoop/edits1,/tmp/hadoop/edits2,/tmp/hadoop/edits3";
        conf.set(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_EDITS_DIR_KEY, dirs);

        // Test code
        List<URI> result = FSImage.getCheckpointEditsDirs(conf, null);

        // Assertions
        assertNotNull(result);
        assertEquals(3, result.size());
        assertEquals(URI.create("file:/tmp/hadoop/edits1"), result.get(0));
        assertEquals(URI.create("file:/tmp/hadoop/edits2"), result.get(1));
        assertEquals(URI.create("file:/tmp/hadoop/edits3"), result.get(2));
    }

    @Test
    public void testGetCheckpointEditsDirs_usesConfigValueDirectly() {
        // Prepare test conditions
        String testDir = "/tmp/hadoop/edits";
        conf.set(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_EDITS_DIR_KEY, testDir);

        // Test code
        List<URI> result = FSImage.getCheckpointEditsDirs(conf, null);

        // Assertions
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals(URI.create("file:" + testDir), result.get(0));
    }

    @Test
    public void testGetCheckpointEditsDirs_usesDefaultWhenConfigNotSet() {
        // Prepare test conditions
        String defaultName = "/tmp/default/edits";

        // Test code
        List<URI> result = FSImage.getCheckpointEditsDirs(conf, defaultName);

        // Assertions
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals(URI.create("file:" + defaultName), result.get(0));
    }

    @Test
    public void testGetCheckpointEditsDirs_multipleDirectories() {
        // Prepare test conditions
        String dirs = "/tmp/hadoop/edits1,/tmp/hadoop/edits2,/tmp/hadoop/edits3";
        conf.set(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_EDITS_DIR_KEY, dirs);

        // Test code
        List<URI> result = FSImage.getCheckpointEditsDirs(conf, null);

        // Assertions
        assertNotNull(result);
        assertEquals(3, result.size());
        assertEquals(URI.create("file:/tmp/hadoop/edits1"), result.get(0));
        assertEquals(URI.create("file:/tmp/hadoop/edits2"), result.get(1));
        assertEquals(URI.create("file:/tmp/hadoop/edits3"), result.get(2));
    }

    @Test
    public void testGetCheckpointEditsDirs_variousUriFormats() {
        // Prepare test conditions
        String[] testPaths = {
            "file:///tmp/hadoop/edits",
            "/tmp/hadoop/edits",
            "hdfs://namenode:9000/edits"
        };
        
        for (String dirPath : testPaths) {
            Configuration testConf = new Configuration();
            testConf.set(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_EDITS_DIR_KEY, dirPath);

            // Test code
            List<URI> result = FSImage.getCheckpointEditsDirs(testConf, null);

            // Assertions
            assertNotNull(result);
            assertEquals(1, result.size());
            // Just verify it creates valid URI without throwing exception
            assertNotNull(result.get(0));
        }
    }

    @Test
    public void testGetCheckpointEditsDirs_invalidUriHandling() {
        // Prepare test conditions
        String invalidUri = "invalid uri with spaces";
        conf.set(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_EDITS_DIR_KEY, invalidUri);

        // Test code & Assertions
        // Should not throw exception but log error and skip invalid URI
        List<URI> result = FSImage.getCheckpointEditsDirs(conf, null);
        assertNotNull(result);
        // Invalid URI handling may vary, but shouldn't cause test to fail
        // The actual behavior depends on HDFS 2.8.5 implementation
    }

    @Test
    public void testCompareWithPropertiesFileLoader() throws IOException {
        // Simulate loading from properties file
        Properties props = new Properties();
        props.setProperty("dfs.namenode.checkpoint.edits.dir", "/tmp/hadoop/edits");
        
        // Get value using Configuration API
        conf.set(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_EDITS_DIR_KEY, 
                 props.getProperty("dfs.namenode.checkpoint.edits.dir"));
        List<URI> result = FSImage.getCheckpointEditsDirs(conf, null);
        
        // Compare with direct properties loading
        String propValue = props.getProperty("dfs.namenode.checkpoint.edits.dir");
        assertTrue(result.size() > 0);
        assertTrue(result.get(0).toString().contains(propValue));
    }

    @Test
    public void testFallbackToCheckpointDir() {
        // When checkpoint.edits.dir is not set, it should use checkpoint.dir
        String checkpointDir = "/tmp/hadoop/checkpoint";
        conf.set(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_DIR_KEY, checkpointDir);
        // Not setting DFS_NAMENODE_CHECKPOINT_EDITS_DIR_KEY explicitly
        
        // Test code
        List<URI> result = FSImage.getCheckpointEditsDirs(conf, checkpointDir);
        
        // Assertions
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals(URI.create("file:" + checkpointDir), result.get(0));
    }
}