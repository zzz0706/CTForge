package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.FSImage;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FSImageTest {

    private Configuration conf;

    @Before
    public void setUp() {
        conf = new Configuration();
    }

    @Test
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions: Set the configuration value for DFS_NAMENODE_CHECKPOINT_EDITS_DIR_KEY properly.
    // 3. Test logic: Ensure that the FSImage.getCheckpointEditsDirs API correctly parses the configuration into URI paths.
    // 4. Validate the test results with assertions.
    public void checkpointEditsHandlesNullDefaultNameWhenDirNamesPresent() {
        // Prepare Test Conditions
        conf.set(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_EDITS_DIR_KEY, "file:///tmp/checkpoint");
        String defaultName = null;

        // Execute Test Logic
        List<URI> uriResult = FSImage.getCheckpointEditsDirs(conf, defaultName);

        // Convert URI results to Path for validation
        List<Path> pathResult = new ArrayList<>();
        for (URI uri : uriResult) {
            pathResult.add(new Path(uri.toString()));
        }

        // Expected Result: The configuration is parsed correctly into a list with one element.
        assertEquals(1, pathResult.size());
        assertTrue(pathResult.contains(new Path("file:///tmp/checkpoint")));
    }
}