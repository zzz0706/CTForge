package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.net.URI;
import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FSNamesystemConfigurationTest {

    @Test
    // Test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void test_getNamespaceEditsDirs_WithSharedEditsIncluded() throws Exception {
        // Prepare the test conditions
        Configuration conf = new Configuration();
        Path sharedEditsPath = new Path("file:///tmp/shared_edits");
        conf.set(DFSConfigKeys.DFS_NAMENODE_SHARED_EDITS_DIR_KEY, sharedEditsPath.toString());

        // Test code
        List<URI> result = FSNamesystem.getNamespaceEditsDirs(conf, true);

        // Assert that the shared edits directory is included in the result
        assertTrue("Shared edits directory must be included in the namespace edits directories.",
                result.contains(sharedEditsPath.toUri()));
    }

    @Test
    // Test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void test_getNamespaceEditsDirsWithoutSharedEdits_RemovesSharedEditsDir() throws Exception {
        // Prepare the test conditions
        Configuration conf = new Configuration();
        Path sharedEditsPath = new Path("file:///tmp/shared_edits");
        Path nonSharedEditsPath = new Path("file:///tmp/non_shared_edits");
        conf.set(DFSConfigKeys.DFS_NAMENODE_SHARED_EDITS_DIR_KEY, sharedEditsPath.toString());
        conf.setStrings(DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY, nonSharedEditsPath.toString());

        // Test code
        List<URI> result = FSNamesystem.getNamespaceEditsDirs(conf, false);

        // Assert that the non-shared edits directory is included and the shared edits directory is excluded
        assertTrue("Non-shared edits directory must be included in the namespace edits directories.",
                result.contains(nonSharedEditsPath.toUri()));
        assertFalse("Shared edits directory must be excluded from the namespace edits directories.",
                result.contains(sharedEditsPath.toUri()));
    }

    @Test
    // Test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void test_getSharedEditsDirs_ReturnsConfiguredSharedEditsDir() {
        // Prepare the test conditions
        Configuration conf = new Configuration();
        Path sharedEditsPath = new Path("file:///tmp/shared_edits");
        conf.set(DFSConfigKeys.DFS_NAMENODE_SHARED_EDITS_DIR_KEY, sharedEditsPath.toString());

        // Test code
        List<URI> result = FSNamesystem.getSharedEditsDirs(conf);

        // Assert the shared edits directory is correctly returned
        assertTrue("Shared edits directory should return the configured value.",
                result.contains(sharedEditsPath.toUri()));
    }
}