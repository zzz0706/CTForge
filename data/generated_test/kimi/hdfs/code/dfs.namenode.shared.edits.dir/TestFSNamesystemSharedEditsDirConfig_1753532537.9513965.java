package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.List;

import static org.junit.Assert.*;

public class TestFSNamesystemSharedEditsDirConfig {

    @Test
    // testMultipleSharedEditsDirsThrowsIOException
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testMultipleSharedEditsDirsThrowsIOException() {
        Configuration conf = new Configuration();
        // Set multiple shared edits directories in the configuration
        String sharedEditsDirs = "file:///shared/edits1,file:///shared/edits2";
        conf.set(DFSConfigKeys.DFS_NAMENODE_SHARED_EDITS_DIR_KEY, sharedEditsDirs);

        try {
            // This should throw an IOException because multiple shared edits directories are not supported
            List<URI> result = FSNamesystem.getNamespaceEditsDirs(conf);
            fail("Expected IOException when multiple shared edits directories are configured");
        } catch (IOException e) {
            // Verify that the exception message indicates that multiple shared edits directories are not supported
            assertTrue("Exception message should indicate multiple shared edits directories are not supported",
                    e.getMessage().contains("Multiple shared edits directories are not yet supported"));
        }
    }
}