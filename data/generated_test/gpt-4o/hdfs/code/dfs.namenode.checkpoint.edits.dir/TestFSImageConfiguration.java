package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.FSImage;

import org.junit.Test;

import java.net.URI;
import java.util.List;

import static org.junit.Assert.assertTrue;

public class TestFSImageConfiguration {

    @Test
    // Test code
    // 1. You need to use the HDFS 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code to verify FSImage method for checkpoint edits directories.
    // 4. Code after testing.

    public void testValidCheckpointEditsDirsWithDefault() {
        // Step 1: Prepare the configuration object with necessary settings.
        Configuration conf = new Configuration();
        
        // Step 2: Set the default checkpoint edits directory key for testing.
        String expectedDir = "/tmp/checkpoint";
        // Ensure correct URI format with "file://" prefix
        URI expectedUri = URI.create("file://" + expectedDir);
        conf.set(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_EDITS_DIR_KEY, expectedDir);

        // Step 3: Invoke the FSImage API method to retrieve checkpoint edits directories.
        List<URI> checkpointEditsDirs = FSImage.getCheckpointEditsDirs(conf, null);

        // Step 4: Verify the result - Ensure the returned list contains the URI matching the expected directory.
        boolean containsExpectedDir = false;
        for (URI uri : checkpointEditsDirs) {
            if (uri.equals(expectedUri)) {
                containsExpectedDir = true;
                break;
            }
        }
        
        assertTrue("The returned list should contain the expected directory as URI.", containsExpectedDir);
    }
}