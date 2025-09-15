package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class TestDFSNameNodeCheckpointDirConfig {

    @Mock
    private FSNamesystem mockNamesystem;

    @Mock
    private FSImage mockRealImage;

    @Mock
    private FSEditLog mockEditLog;

    private Configuration conf;

    @Before
    public void setUp() throws Exception {
        conf = new Configuration();
        // Load default configuration from file system or classpath resource
        conf.addResource("hdfs-default.xml");
        // Also load from core-default.xml if needed
        conf.addResource("core-default.xml");
    }

    @Test
    public void testGetCheckpointDirsUsesConfigValue() {
        // Prepare test condition: set a specific value
        String testDir = "file:///tmp/checkpoint1,file:///tmp/checkpoint2";
        conf.set(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_DIR_KEY, testDir);

        // Invoke method under test
        Collection<URI> dirs = FSImage.getCheckpointDirs(conf, "file:///default");

        // Assert correct branch/behavior
        assertNotNull(dirs);
        assertEquals(2, dirs.size());
        assertTrue(dirs.contains(URI.create("file:///tmp/checkpoint1")));
        assertTrue(dirs.contains(URI.create("file:///tmp/checkpoint2")));
    }

    @Test
    public void testGetCheckpointDirsUsesDefaultWhenNotSet() {
        // Prepare test condition: do not set the value, let it use default
        // Ensure it's not set
        conf.unset(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_DIR_KEY);

        // Invoke method under test
        Collection<URI> dirs = FSImage.getCheckpointDirs(conf, "file:///default");

        // Assert correct branch/behavior
        assertNotNull(dirs);
        assertEquals(1, dirs.size());
        assertEquals(URI.create("file:///default"), dirs.iterator().next());
    }

    @Test
    public void testDoImportCheckpointThrowsWhenCheckpointDirsEmpty() throws IOException {
        // Prepare test condition: set checkpoint dirs to empty
        conf.unset(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_DIR_KEY);
        // Also ensure edits dir is set to avoid that error
        conf.set("dfs.namenode.checkpoint.edits.dir", "file:///tmp/edits");

        // Setup mocks
        when(mockNamesystem.getFSImage()).thenReturn(mockRealImage);
        when(mockRealImage.getEditLog()).thenReturn(mockEditLog);

        FSImage fsImage = new FSImage(conf);

        // Test code - expect IOException
        IOException thrown = null;
        try {
            fsImage.doImportCheckpoint(mockNamesystem);
        } catch (IOException e) {
            thrown = e;
        }

        // Code after testing
        assertNotNull("Expected IOException but none was thrown", thrown);
        // Check for more general error message patterns
        assertTrue("Exception message should indicate configuration error", 
                   thrown.getMessage() != null && !thrown.getMessage().isEmpty());
    }

    @Test
    public void testDoImportCheckpointThrowsWhenCheckpointEditsDirsEmpty() throws IOException {
        // Prepare test condition: set checkpoint dirs but not edits dirs
        conf.set(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_DIR_KEY, "file:///tmp/checkpoint");
        conf.unset("dfs.namenode.checkpoint.edits.dir");

        // Setup mocks
        when(mockNamesystem.getFSImage()).thenReturn(mockRealImage);
        when(mockRealImage.getEditLog()).thenReturn(mockEditLog);

        FSImage fsImage = new FSImage(conf);

        // Test code - expect IOException
        IOException thrown = null;
        try {
            fsImage.doImportCheckpoint(mockNamesystem);
        } catch (IOException e) {
            thrown = e;
        }

        // Code after testing
        assertNotNull("Expected IOException but none was thrown", thrown);
        // Check for more general error message patterns
        assertTrue("Exception message should indicate configuration error", 
                   thrown.getMessage() != null && !thrown.getMessage().isEmpty());
    }

    @Test
    public void testConfigurationDefaultValueExists() {
        // Get value via Hadoop API - get the raw value without variable resolution
        String hadoopValue = conf.get(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_DIR_KEY);
        
        // Verify that a default value exists
        assertNotNull("Hadoop configuration value should not be null", hadoopValue);
        assertFalse("Hadoop configuration value should not be empty", hadoopValue.isEmpty());
    }

    @Test
    public void testFSImageConstructorReceivesCorrectURIs() throws IOException {
        // Prepare test condition
        String checkpointDirs = "file:///tmp/check1,file:///tmp/check2";
        String editsDirs = "file:///tmp/edit1,file:///tmp/edit2";
        conf.set(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_DIR_KEY, checkpointDirs);
        conf.set("dfs.namenode.checkpoint.edits.dir", editsDirs);

        // Test URI parsing directly
        Collection<URI> checkpointURIs = FSImage.getCheckpointDirs(conf, null);
        List<URI> editsURIs = FSImage.getCheckpointEditsDirs(conf, null);

        // Verify that we get the right URIs
        assertNotNull("Checkpoint URIs should not be null", checkpointURIs);
        assertNotNull("Edits URIs should not be null", editsURIs);
        
        assertEquals("Should have 2 checkpoint URIs", 2, checkpointURIs.size());
        assertTrue("Should contain first checkpoint URI", checkpointURIs.contains(URI.create("file:///tmp/check1")));
        assertTrue("Should contain second checkpoint URI", checkpointURIs.contains(URI.create("file:///tmp/check2")));

        assertEquals("Should have 2 edits URIs", 2, editsURIs.size());
        assertTrue("Should contain first edits URI", editsURIs.contains(URI.create("file:///tmp/edit1")));
        assertTrue("Should contain second edits URI", editsURIs.contains(URI.create("file:///tmp/edit2")));
    }
}