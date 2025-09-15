package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.namenode.FSImage;
import org.junit.Test;
import java.net.URI;
import java.util.Collection;
import static org.junit.Assert.*;

public class TestFSImageConfiguration {
    // Get configuration value using API
    // Prepare the input conditions for unit testing.
    @Test
    public void test_getCheckpointDirs_WithEmptyConfiguration() {
        // Create a Configuration object without any set values
        Configuration conf = new Configuration();

        // Ensure dfs.namenode.checkpoint.dir is unset
        Collection<URI> checkpointDirs = FSImage.getCheckpointDirs(conf, null);

        // Verify the expected outcome when no configuration or default values exist
        assertTrue("Checkpoint directories should be empty as no configuration values are provided.", checkpointDirs.isEmpty());
    }
}