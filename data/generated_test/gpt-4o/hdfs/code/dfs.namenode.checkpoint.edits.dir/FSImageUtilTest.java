package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.common.Util;
import org.apache.hadoop.hdfs.server.namenode.FSImage;
import org.junit.Test;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertFalse;

import java.net.URI;
import java.util.Arrays; // Corrected import for Arrays.asList
import java.util.Collection;
import java.util.List;

public class FSImageUtilTest {

    // Test case to verify FSImage.getCheckpointEditsDirs functionality
    @Test
    public void test_getCheckpointEditsDirs_withConfiguredDirectories() {
        // Get configuration value using API
        Configuration conf = new Configuration();
        conf.set("dfs.namenode.checkpoint.edits.dir", "file:///tmp/checkpointEditsDir,file:///tmp/extraDir");

        // Prepare the input conditions for unit testing
        String defaultName = null;

        // Call the method to test FSImage.getCheckpointEditsDirs
        List<URI> checkpointEditsDirs = FSImage.getCheckpointEditsDirs(conf, defaultName);

        // Test the code and verify output
        assertNotNull("The list of checkpoint edits directories should not be null", checkpointEditsDirs);
        assertFalse("The list of checkpoint edits directories should not be empty", checkpointEditsDirs.isEmpty());

        // Validate the URIs retrieved from the configuration
        for (URI uri : checkpointEditsDirs) {
            assertNotNull("Each URI in the checkpoint edits directories list should be valid and not null", uri);
        }
    }

    // Test case to verify Util.stringCollectionAsURIs functionality
    @Test
    public void test_stringCollectionAsURIs() {
        // Prepare the input conditions
        Collection<String> dirNames = Arrays.asList("file:///tmp/checkpointEditsDir", "file:///tmp/extraDir");

        // Call the method to convert strings into URIs
        List<URI> uris = Util.stringCollectionAsURIs(dirNames);

        // Test the code and verify output
        assertNotNull("The list of URIs should not be null", uris);
        assertFalse("The list of URIs should not be empty", uris.isEmpty());

        // Validate individual URI objects
        for (URI uri : uris) {
            assertNotNull("Each URI in the list should be valid and not null", uri);
        }
    }
}