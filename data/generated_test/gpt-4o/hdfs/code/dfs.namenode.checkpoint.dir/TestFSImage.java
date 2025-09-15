package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.namenode.FSImage;
import org.apache.hadoop.hdfs.server.common.JspHelper;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Test;

import java.net.URI;
import java.util.Collection;

import static org.junit.Assert.assertTrue;

public class TestFSImage {

    @Test
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void test_getCheckpointDirs_withValidConfiguration() throws Exception {
        // 1. Prepare the configuration object and set the checkpoint directory
        Configuration config = new HdfsConfiguration();
        config.set(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_DIR_KEY, "file:///tmp/checkpoint");

        // 2. Invoke FSImage.getCheckpointDirs method
        Collection<URI> checkpointDirs = FSImage.getCheckpointDirs(config, null);

        // 3. Assert that the result contains the expected URI
        assertTrue("Expected checkpoint directory is missing in the result",
                checkpointDirs.contains(URI.create("file:///tmp/checkpoint")));
    }
}