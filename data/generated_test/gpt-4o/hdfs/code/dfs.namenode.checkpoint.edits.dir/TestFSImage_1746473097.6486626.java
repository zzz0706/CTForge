package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.common.Util;
import org.junit.Test;

import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestFSImage {

    @Test
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testValidCheckpointEditsDirsFromConfiguration() {
        // Prepare the test conditions
        Configuration conf = new Configuration();
        // Set the `dfs.namenode.checkpoint.edits.dir` key using the Configuration object
        conf.set(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_EDITS_DIR_KEY, "file:/pathA,file:/pathB,file:/pathC");

        // Test code: Call the function to process the configuration
        List<URI> checkpointEditDirs = FSImage.getCheckpointEditsDirs(conf, null);

        // Validate the results: Verify that the returned list matches the configured URIs
        assertEquals(3, checkpointEditDirs.size());
        assertEquals(URI.create("file:/pathA"), checkpointEditDirs.get(0));
        assertEquals(URI.create("file:/pathB"), checkpointEditDirs.get(1));
        assertEquals(URI.create("file:/pathC"), checkpointEditDirs.get(2));
    }

    @Test
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testDefaultCheckpointEditsDirsFromConfiguration() {
        // Prepare the test conditions
        Configuration conf = new Configuration();
        // Do not set the `dfs.namenode.checkpoint.edits.dir` key
        String defaultDir = "file:/defaultPath";

        // Test code: Call the function with null configuration key and a default value
        List<URI> checkpointEditDirs = FSImage.getCheckpointEditsDirs(conf, defaultDir);

        // Validate the results: Verify that the default name is used
        assertEquals(1, checkpointEditDirs.size());
        assertEquals(URI.create(defaultDir), checkpointEditDirs.get(0));
    }

    @Test
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testStringCollectionAsURIs() {
        // Prepare the test conditions: Use a collection of strings simulating paths
        Collection<String> dirNames = Arrays.asList("file:/dir1", "file:/dir2", "file:/dir3");

        // Test code: Use the Util class method to convert strings to URIs
        List<URI> uris = Util.stringCollectionAsURIs(dirNames);

        // Validate the results: Compare URIs array with the input paths
        assertEquals(3, uris.size());
        assertEquals(URI.create("file:/dir1"), uris.get(0));
        assertEquals(URI.create("file:/dir2"), uris.get(1));
        assertEquals(URI.create("file:/dir3"), uris.get(2));
    }
}