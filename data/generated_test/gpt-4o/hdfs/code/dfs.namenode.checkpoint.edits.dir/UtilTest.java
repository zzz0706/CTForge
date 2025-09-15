package org.apache.hadoop.hdfs.server.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Assert;
import org.junit.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class UtilTest {

    /**
     * Test method to verify the conversion of a string collection into a list of URIs.
     */
    @Test
    public void test_stringCollectionAsURIs_validURIs() {
        // Step 1: Use API to fetch configuration value
        Configuration conf = new Configuration();
        Collection<String> dirNames = conf.getTrimmedStringCollection(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_EDITS_DIR_KEY);

        // Step 2: Prepare valid input if configuration is empty
        if (dirNames.isEmpty()) {
            dirNames = new ArrayList<>();
            dirNames.add("/path/to/checkpoint/edits1");
            dirNames.add("/path/to/checkpoint/edits2");
        }

        // Step 3: Call the Util.stringCollectionAsURIs() method with the prepared input
        List<URI> uris = Util.stringCollectionAsURIs(dirNames);

        // Step 4: Verify output - check URI correctness
        Assert.assertEquals("Mismatch in collection and URI size", dirNames.size(), uris.size());
        for (int i = 0; i < dirNames.size(); i++) {
            // Adjust expected URI to match the "file:" scheme used by URI.create
            String expectedURI = URI.create("file:" + dirNames.toArray(new String[0])[i]).toString();
            String actualURI = uris.get(i).toString();
            Assert.assertEquals("Mismatch in URI conversion", expectedURI, actualURI);
        }
    }
}