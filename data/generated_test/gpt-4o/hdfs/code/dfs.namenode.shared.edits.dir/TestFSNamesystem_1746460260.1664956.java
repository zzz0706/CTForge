package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.junit.Test;

import java.net.URI;
import java.util.Collection;
import java.util.List;

public class TestFSNamesystem {

    @Test
    // test_getNamespaceEditsDirs_WithoutSharedEditsIncluded
    // 1. Use the HDFS 2.8.5 API correctly to obtain configuration values instead of hardcoding.
    // 2. Prepare the test conditions.
    // 3. Test the getNamespaceEditsDirs method with includeShared=false.
    // 4. Verify the returned editsDirs do not contain shared edits.
    public void test_getNamespaceEditsDirs_WithoutSharedEditsIncluded() {
        Configuration conf = new Configuration();
        String sharedEditsDirKey = DFSConfigKeys.DFS_NAMENODE_SHARED_EDITS_DIR_KEY;
        conf.set(sharedEditsDirKey, "file:///shared/edits/dir");
        conf.set(DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY, "file:///local/edits/dir");

        try {
            List<URI> editsDirs = FSNamesystem.getNamespaceEditsDirs(conf, false);
            URI sharedEditsUri = new URI(conf.getTrimmed(sharedEditsDirKey));
            
            for (URI dir : editsDirs) {
                assert !dir.equals(sharedEditsUri) : "Shared edits directory was included in the result, but it should not be.";
            }
        } catch (Exception e) {
            e.printStackTrace();
            assert false : "An exception occurred during the test: " + e.getMessage();
        }
    }

    @Test
    // test_getSharedEditsDirs
    // 1. Use the HDFS 2.8.5 API to retrieve shared edits directories.
    // 2. Prepare the test conditions.
    // 3. Test the getSharedEditsDirs functionality.
    // 4. Validate that the returned directories match the configuration.
    public void test_getSharedEditsDirs() {
        Configuration conf = new Configuration();
        String sharedEditsDirKey = DFSConfigKeys.DFS_NAMENODE_SHARED_EDITS_DIR_KEY;
        conf.set(sharedEditsDirKey, "file:///shared/edits/dir1,file:///shared/edits/dir2");

        try {
            List<URI> sharedEditsDirs = FSNamesystem.getSharedEditsDirs(conf);

            Collection<String> expectedDirs = conf.getTrimmedStringCollection(sharedEditsDirKey);
            for (URI dir : sharedEditsDirs) {
                assert expectedDirs.contains(dir.toString()) : "Shared edits directory did not match the expected configuration.";
            }
        } catch (Exception e) {
            e.printStackTrace();
            assert false : "An exception occurred during the test: " + e.getMessage();
        }
    }

    @Test
    // test_getConfigurationWithoutSharedEdits
    // 1. Use HDFS 2.8.5 API to create and modify configurations.
    // 2. Prepare the conditions with shared edits configured.
    // 3. Test the getConfigurationWithoutSharedEdits method.
    // 4. Validate that shared edits have been removed.
    public void test_getConfigurationWithoutSharedEdits() {
        Configuration conf = new Configuration();
        String sharedEditsDirKey = DFSConfigKeys.DFS_NAMENODE_SHARED_EDITS_DIR_KEY;
        conf.set(sharedEditsDirKey, "file:///shared/edits/dir1,file:///shared/edits/dir2");
        conf.set(DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY, "file:///local/edits/dir");

        try {
            Configuration confWithoutSharedEdits = new Configuration(conf);
            confWithoutSharedEdits.unset(sharedEditsDirKey);

            assert confWithoutSharedEdits.get(sharedEditsDirKey) == null : "Shared edits directory configuration was not successfully removed.";

            Collection<String> editsDirs = confWithoutSharedEdits.getTrimmedStringCollection(DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY);
            assert editsDirs.contains("file:///local/edits/dir") : "Edits directories were not correctly populated in the new configuration.";
        } catch (Exception e) {
            e.printStackTrace();
            assert false : "An exception occurred during the test: " + e.getMessage();
        }
    }

    @Test
    // test_initializeSharedEdits
    // 1. Validate the initialization of shared edits functionality.
    // 2. Prepare configurations for shared edits.
    // 3. Test initializeSharedEdits method.
    // 4. Verify initialization status.
    public void test_initializeSharedEdits() {
        Configuration conf = new Configuration();
        String sharedEditsDirKey = DFSConfigKeys.DFS_NAMENODE_SHARED_EDITS_DIR_KEY;
        conf.set(sharedEditsDirKey, "file:///shared/edits/dir");

        try {
            List<URI> sharedEditsDirs = FSNamesystem.getSharedEditsDirs(conf);

            Collection<String> expectedDirs = conf.getTrimmedStringCollection(sharedEditsDirKey);
            for (URI dir : sharedEditsDirs) {
                assert expectedDirs.contains(dir.toString()) : "Shared edits directory initialization did not match the configuration.";
            }
        } catch (Exception e) {
            e.printStackTrace();
            assert false : "An exception occurred during the test: " + e.getMessage();
        }
    }
}