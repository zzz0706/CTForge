package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.common.Util;
import org.apache.hadoop.hdfs.server.namenode.FSImage;
import org.junit.Assert;
import org.junit.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class FSImageUtilTest {

    /**
     * Test method to validate the stringCollectionAsURIs functionality
     * ensuring it properly converts valid string paths to URI objects.
     */
    @Test
    public void test_stringCollectionAsURIs_validURIs() {
        // Prepare the input conditions for unit testing.
        Collection<String> validPaths = new ArrayList<>();
        validPaths.add("/path/to/dir1");
        validPaths.add("/path/to/dir2");
        validPaths.add("/path/to/dir3");

        // Call the Util.stringCollectionAsURIs() method with the valid collection of strings.
        List<URI> uriList = Util.stringCollectionAsURIs(validPaths);

        // Verify the expected behavior: returned list contains correct URI objects.
        Assert.assertEquals("Mismatch between input paths and output URI size", validPaths.size(), uriList.size());
        int index = 0;
        for (String path : validPaths) {
            URI expectedUri = URI.create("file:" + path);
            URI actualUri = uriList.get(index++);
            Assert.assertEquals("Mismatch in URI conversion", expectedUri, actualUri);
        }
    }

    /**
     * Test method to validate the FSImage.getCheckpointEditsDirs method
     * ensuring it uses configuration and default name correctly.
     */
    @Test
    public void test_getCheckpointEditsDirs_validConfigurationAndDefaultName() {
        // Step 1: Use API to fetch configuration value
        Configuration conf = new Configuration();
        conf.setStrings(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_EDITS_DIR_KEY, 
                        "/path/to/checkpoint/edits1", 
                        "/path/to/checkpoint/edits2");

        // Call FSImage.getCheckpointEditsDirs method with configuration and default name
        String defaultName = "/fallback/path";
        List<URI> uris = FSImage.getCheckpointEditsDirs(conf, defaultName);

        // Step 3: Verify output - check size and correctness
        Collection<String> configuredDirNames = conf.getTrimmedStringCollection(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_EDITS_DIR_KEY);
        Collection<String> allDirNames = new ArrayList<>(configuredDirNames);
        if (configuredDirNames.isEmpty()) {
            allDirNames.add(defaultName);
        }

        Assert.assertEquals("Mismatch between directories and URI size", allDirNames.size(), uris.size());
        for (int i = 0; i < allDirNames.size(); i++) {
            String expectedUriString = URI.create("file:" + allDirNames.toArray(new String[0])[i]).toString();
            String actualUriString = uris.get(i).toString();
            Assert.assertEquals("Mismatch in URI conversion", expectedUriString, actualUriString);
        }
    }
}