package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.common.Util;
import org.junit.Test;

import java.net.URI;
import java.util.List;

public class TestFSImage {

    @Test
    // Test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testEmptyCheckpointEditsDirs() {
        // Prepare the test conditions
        Configuration conf = new Configuration(); // Create an empty Configuration object

        // Test code
        List<URI> checkpointEditsDirs = FSImage.getCheckpointEditsDirs(conf, null); // Call the method with null as the default directory value

        // Assertions
        assert checkpointEditsDirs.isEmpty(); // Verify that the returned list is empty
    }
}