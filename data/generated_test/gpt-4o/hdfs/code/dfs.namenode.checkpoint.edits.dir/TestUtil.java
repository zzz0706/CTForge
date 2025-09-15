package org.apache.hadoop.hdfs.server.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class TestUtil {
    // get configuration value using API
    // Prepare the input conditions for unit testing.
    @Test
    public void test_stringCollectionAsURIs_withInvalidURI() {
        // Obtain configuration value using API
        Configuration conf = new Configuration();
        Collection<String> dirNames = conf.getTrimmedStringCollection(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_EDITS_DIR_KEY);
        
        // Prepare input with valid and invalid URIs
        List<String> inputDirectories = new ArrayList<>();
        inputDirectories.add("file:///valid/path");
        inputDirectories.add("invalid:?path");   // Invalid URI
        inputDirectories.add("file:///another/valid/path");
        dirNames.addAll(inputDirectories);

        // Pass the collection to stringCollectionAsURIs method.
        List<URI> uris = Util.stringCollectionAsURIs(dirNames);

        // Verify that valid URIs are returned while invalid ones are gracefully handled.
        for (URI uri : uris) {
            System.out.println("Valid URI: " + uri.toString());
        }
        // Check logs for errors related to the invalid URI.
        // You can validate that invalid URIs do not crash the function and are skipped.
    }
}