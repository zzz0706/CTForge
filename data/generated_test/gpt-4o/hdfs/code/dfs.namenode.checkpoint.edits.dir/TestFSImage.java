package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.common.Util;
import org.junit.Test;

import java.net.URI;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestFSImage {

    @Test
    // Test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testValidCheckpointEditsDirsFromConfiguration() {
        // Prepare the test conditions
        Configuration conf = new Configuration();
        // Set the `dfs.namenode.checkpoint.edits.dir` key using the Configuration object
        conf.set(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_EDITS_DIR_KEY, "file:/path1,file:/path2,file:/path3");

        // Test code: Call the function to process the configuration
        List<URI> checkpointEditsDirs = FSImage.getCheckpointEditsDirs(conf, null);

        // Validate the results: Verify that the returned list contains 3 URI objects representing 'file:/path1', 'file:/path2', and 'file:/path3'
        assertEquals(3, checkpointEditsDirs.size());
        assertEquals(URI.create("file:/path1"), checkpointEditsDirs.get(0));
        assertEquals(URI.create("file:/path2"), checkpointEditsDirs.get(1));
        assertEquals(URI.create("file:/path3"), checkpointEditsDirs.get(2));
    }
}