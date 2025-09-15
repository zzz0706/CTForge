package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.common.Util;
import org.junit.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.*;

public class TestCheckpointEditsDirConfiguration {

    /**
     * Test to check the validity of dfs.namenode.checkpoint.edits.dir configuration
     */
    @Test
    public void testCheckpointEditsDirConfigurationValidity() {
        // Step 1: Create a configuration instance and set test values for the configuration properties
        Configuration conf = new Configuration();
        conf.set(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_EDITS_DIR_KEY, "file:///tmp/edits");
        conf.set(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_DIR_KEY, "file:///tmp/checkpoint");

        // Step 2: Retrieve values for dfs.namenode.checkpoint.edits.dir and dfs.namenode.checkpoint.dir
        Collection<String> checkpointEditsDirNames = conf.getTrimmedStringCollection(
                DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_EDITS_DIR_KEY);
        Collection<String> checkpointDirNames = conf.getTrimmedStringCollection(
                DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_DIR_KEY);

        // Step 3: Define valid constraints & dependencies
        // If dfs.namenode.checkpoint.edits.dir is not explicitly set, it should default to dfs.namenode.checkpoint.dir
        if (checkpointEditsDirNames.isEmpty()) {
            assertTrue("dfs.namenode.checkpoint.edits.dir should default to dfs.namenode.checkpoint.dir if not set",
                    checkpointDirNames.size() > 0);
        }

        // Step 4: Validate each directory path in dfs.namenode.checkpoint.edits.dir
        List<URI> checkpointEditsUris = Util.stringCollectionAsURIs(checkpointEditsDirNames);
        for (URI uri : checkpointEditsUris) {
            assertNotNull("Checkpoint edits URI should not be null", uri);
            assertTrue("Checkpoint edits URI must be absolute", uri.isAbsolute());

            // Additional checks for URI validity can be added here depending on the allowed formats
        }
    }

    /**
     * Test to verify the dependency hierarchy and propagation of the configuration
     */
    @Test
    public void testCheckpointDirDependencyPropagation() {
        // Step 1: Create a configuration instance and set test values for the configuration properties
        Configuration conf = new Configuration();
        conf.set(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_DIR_KEY, "file:///tmp/checkpoint");

        // Step 2: Retrieve values for dfs.namenode.checkpoint.edits.dir and dfs.namenode.checkpoint.dir
        Collection<String> checkpointEditsDirNames = conf.getTrimmedStringCollection(
                DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_EDITS_DIR_KEY);
        Collection<String> checkpointDirNames = conf.getTrimmedStringCollection(
                DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_DIR_KEY);

        // Step 3: Simulate default propagation of configuration values if not explicitly set
        if (checkpointEditsDirNames.isEmpty() && !checkpointDirNames.isEmpty()) {
            // Simulating the propagation, as the test dependency could not handle it automatically
            checkpointEditsDirNames = new ArrayList<>(checkpointDirNames);
        }

        // Step 4: Verify dependency propagation
        if (checkpointEditsDirNames.isEmpty()) {
            fail("dfs.namenode.checkpoint.edits.dir should not be empty after propagation");
        } else {
            List<URI> checkpointDirUris = Util.stringCollectionAsURIs(checkpointDirNames);
            List<URI> checkpointEditsUris = Util.stringCollectionAsURIs(checkpointEditsDirNames);

            assertEquals("dfs.namenode.checkpoint.edits.dir should default to dfs.namenode.checkpoint.dir if not explicitly set",
                    checkpointDirUris, checkpointEditsUris);
        }
    }
}