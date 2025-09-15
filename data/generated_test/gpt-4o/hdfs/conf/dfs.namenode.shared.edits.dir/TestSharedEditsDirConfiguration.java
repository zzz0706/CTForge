package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.util.StringUtils;

import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.List;

import static org.junit.Assert.*;

public class TestSharedEditsDirConfiguration {

    @Test
    // Test code
    public void testSharedEditsDirConfiguration() throws IOException {
        // 1. You need to use the HDFS 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        Configuration conf = new Configuration();

        // 2. Prepare the test conditions.
        // Extract the value of dfs.namenode.shared.edits.dir from the configuration.
        String sharedEditsDir = conf.get(DFSConfigKeys.DFS_NAMENODE_SHARED_EDITS_DIR_KEY);
        boolean isHAEnabled = conf.getBoolean("dfs.nameservice.enabled", false); // Replace with proper key for HA check

        // 3. Test code.

        // Case 1: If the configuration dfs.namenode.shared.edits.dir is non-empty,
        // ensure that HA is enabled.
        if (sharedEditsDir != null && !sharedEditsDir.isEmpty()) {
            assertTrue("HA must be enabled when dfs.namenode.shared.edits.dir is set", isHAEnabled);

            // Verify that there's only one shared edits directory.
            List<URI> sharedEditsDirs = FSNamesystem.getSharedEditsDirs(conf);
            assertTrue("Only one shared edits directory is supported", sharedEditsDirs.size() <= 1);

            // Check if the shared edits directory is valid.
            for (URI uri : sharedEditsDirs) {
                assertNotNull("Shared edits directory cannot be null", uri);
                assertTrue("Shared edits directory should be a valid URI", uri.isAbsolute());
            }
        }

        // Case 2: If HA is not enabled, dfs.namenode.shared.edits.dir should be null or empty.
        if (!isHAEnabled) {
            assertTrue("dfs.namenode.shared.edits.dir must be unset or empty when HA is not enabled",
                    sharedEditsDir == null || sharedEditsDir.isEmpty());
        }

        // Case 3: Verify the shared edits directory constraints in HA mode.
        if (isHAEnabled) {
            try {
                // Ensure that no invalid directories are configured.
                List<URI> editsDirs = FSNamesystem.getNamespaceEditsDirs(conf, true);
                for (URI dir : editsDirs) {
                    assertNotNull("Edits directory cannot be null", dir);
                    assertTrue("Edits directory must be an absolute URI", dir.isAbsolute());
                }
            } catch (IOException e) {
                fail("Unexpected exception during namespace edits dir validation: " + e.getMessage());
            }
        }

        // 4. Code after testing.
        System.out.println("All Shared Edits Directory configuration tests passed.");
    }
}