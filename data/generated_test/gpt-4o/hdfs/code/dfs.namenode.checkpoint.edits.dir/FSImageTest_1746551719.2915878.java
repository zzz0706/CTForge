package org.apache.hadoop.hdfs.server.namenode;

import org.junit.Before;
import org.junit.Test;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.common.Util;
import org.apache.hadoop.hdfs.DFSConfigKeys;

import java.net.URI;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class FSImageTest {

    private Configuration conf;

    @Before
    public void setUp() {
        conf = new Configuration();
    }

    @Test
    public void checkpointEditsDefaultNameNotAddedWhenDirNamesPresent() {
        // Step 1: Create a Configuration object `conf` and set valid string paths for DFS_NAMENODE_CHECKPOINT_EDITS_DIR_KEY.
        String dirPath = "file:///tmp/checkpoint";
        conf.set(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_EDITS_DIR_KEY, dirPath);

        // Step 2: Set a non-null string value for the `defaultName` parameter.
        String defaultName = "hdfs://localhost:8020";

        // Step 3: Invoke the getCheckpointEditsDirs function with `conf` and `defaultName`.
        List<URI> result = FSImage.getCheckpointEditsDirs(conf, defaultName);

        // Step 4: Capture and assert the results.
        assertEquals("The resulting list should contain only the URIs corresponding to dirPath.", 1, result.size());
        assertEquals("file:///tmp/checkpoint", result.get(0).toString());
    }
}