package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.namenode.FSImage;
import org.apache.hadoop.util.StringUtils;

import java.net.URI;
import java.util.Collection;

public class TestFSImage {

    @org.junit.Test
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void test_getCheckpointDirs_withEmptyConfigurationAndDefaultValue() {
        // 2. Prepare the test conditions.
        Configuration configuration = new Configuration(); // Creates a configuration object.
        String defaultCheckpointDir = "file:///default-checkpoint"; // Specify the default checkpoint directory.
        
        // 3. Test code.
        Collection<URI> checkpointDirs = FSImage.getCheckpointDirs(configuration, defaultCheckpointDir);
        
        // Assert that the returned collection contains the default checkpoint directory as a URI.
        assert checkpointDirs.size() == 1 : "Expected collection to contain one URI.";
        assert checkpointDirs.iterator().next().toString().equals(defaultCheckpointDir) : 
            "Expected directory URI to match the default checkpoint directory value.";
        
        // 4. Code after testing.
        // No additional cleanup is necessary for this scenario.
    }
}