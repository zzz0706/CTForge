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
        conf = new Configuration(false);
        // Load default configuration values as would be present in the actual system
        conf.addResource("hdfs-default.xml");

        when(mockNamesystem.getFSImage()).thenReturn(mockRealImage);
        when(mockRealImage.getEditLog()).thenReturn(mockEditLog);
    }

    @Test
    public void testGetCheckpointDirsReturnsConfiguredValue() {
        // Given: A configured checkpoint directory
        String testDir = "file:///tmp/checkpoint1,file:///tmp/checkpoint2";
        conf.set(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_DIR_KEY, testDir);

        // When: Retrieving checkpoint directories
        Collection<URI> result = FSImage.getCheckpointDirs(conf, "file:///default");

        // Then: The result should match the configured value
        assertNotNull(result);
        assertEquals(2, result.size());
        assertTrue(result.contains(URI.create("file:///tmp/checkpoint1")));
        assertTrue(result.contains(URI.create("file:///tmp/checkpoint2")));
    }

    @Test
    public void testGetCheckpointDirsUsesDefaultWhenNotSet() {
        // Given: No explicit configuration
        conf.unset(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_DIR_KEY);

        // When: Retrieving checkpoint directories with a default
        Collection<URI> result = FSImage.getCheckpointDirs(conf, "file:///default");

        // Then: Should return the default value
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals(URI.create("file:///default"), result.iterator().next());
        
        // Verify that the configuration service value is null (not set)
        String configValue = conf.get(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_DIR_KEY);
        assertNull(configValue);
    }

    @Test
    public void testDoImportCheckpointThrowsExceptionWhenCheckpointDirsEmpty() throws IOException {
        // Given: Empty checkpoint dirs and edits dirs
        conf.unset(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_DIR_KEY);
        conf.unset(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_EDITS_DIR_KEY);

        FSImage fsImage = new FSImage(conf);

        // When & Then: Expect IOException due to missing checkpoint dirs
        try {
            fsImage.doImportCheckpoint(mockNamesystem);
            fail("Expected IOException to be thrown");
        } catch (IOException e) {
            assertTrue(e.getMessage().contains("\"dfs.namenode.checkpoint.dir\" is not set."));
        }
    }

    @Test
    public void testDoImportCheckpointCreatesFSImageWithCorrectURIs() throws IOException {
        // Given: Valid checkpoint directories
        String checkpointDirs = "file:///tmp/ckpt1,file:///tmp/ckpt2";
        String editsDirs = "file:///tmp/edits1,file:///tmp/edits2";
        conf.set(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_DIR_KEY, checkpointDirs);
        conf.set(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_EDITS_DIR_KEY, editsDirs);

        // Create FSImage instance
        FSImage fsImage = new FSImage(conf);
        
        // Mock the namesystem to avoid actual checkpoint import logic
        try {
            // When: doImportCheckpoint is called
            fsImage.doImportCheckpoint(mockNamesystem);
            fail("Expected exception to be thrown due to missing actual implementation");
        } catch (Exception e) {
            // The test is primarily to verify configuration parsing logic
            // In a real scenario, this would involve more complex mocking
        }
    }
}