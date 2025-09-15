package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Assert;
import org.junit.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class FSImageTest {

    /**
     * Test method to verify usage of getCheckpointEditsDirs method 
     * with configuration and default name, ensuring proper URI conversion.
     */
    @Test
    public void test_getCheckpointEditsDirs_validConfigurationAndDefaultName() {
        // Step 1: Use API to fetch configuration value
        Configuration conf = new Configuration();
        conf.setStrings(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_EDITS_DIR_KEY, 
                        "/path/to/checkpoint/edits1", 
                        "/path/to/checkpoint/edits2");

        // Step 2: Call the FSImage.getCheckpointEditsDirs() method with configuration and default name
        String defaultName = "/fallback/path";
        List<URI> uris = FSImage.getCheckpointEditsDirs(conf, defaultName);

        // Step 3: Verify output - check size and URI correctness
        Collection<String> configuredDirNames = conf.getTrimmedStringCollection(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_EDITS_DIR_KEY);
        Collection<String> allDirNames = new ArrayList<>(configuredDirNames);
        if (configuredDirNames.isEmpty()) {
            allDirNames.add(defaultName);
        }

        Assert.assertEquals("Mismatch in collection and URI size", allDirNames.size(), uris.size());
        for (int i = 0; i < allDirNames.size(); i++) {
            String expectedURI = URI.create("file:" + allDirNames.toArray(new String[0])[i]).toString();
            String actualURI = uris.get(i).toString();
            Assert.assertEquals("Mismatch in URI conversion", expectedURI, actualURI);
        }
    }
}