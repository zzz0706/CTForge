package org.apache.hadoop.hdfs.server.namenode;

import org.junit.Before;
import org.junit.Test;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.namenode.FSImage;
import static org.junit.Assert.assertEquals;

import java.net.URI;
import java.util.List;

public class FSImageTest {

    private Configuration conf;

    @Before
    public void setUp() {
        conf = new Configuration();
    }

    @Test
    public void checkpointEditsDefaultNameAddedWhenDirNamesEmpty() {
        // Prerequisites: Ensure the configuration object `conf` does not contain any values for DFS_NAMENODE_CHECKPOINT_EDITS_DIR_KEY.
        conf.unset("dfs.namenode.checkpoint.edits.dir"); 

        // Step 1: Set a non-null string value for the `defaultName` parameter.
        String defaultName = "hdfs://localhost:8020";

        // Step 2: Invoke the getCheckpointEditsDirs function with `conf` and `defaultName`.
        List<URI> result = FSImage.getCheckpointEditsDirs(conf, defaultName);

        // Step 3: Capture the resulting list of URI objects and verify the expected result.
        assertEquals(1, result.size());
        assertEquals(URI.create(defaultName), result.get(0));
    }
}