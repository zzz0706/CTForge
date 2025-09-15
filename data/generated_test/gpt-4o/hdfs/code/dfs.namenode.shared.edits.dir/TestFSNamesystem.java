package org.apache.hadoop.hdfs.server.namenode; 

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.junit.Test;
import java.net.URI;
import java.util.List;

public class TestFSNamesystem {

    @Test
    // test_getNamespaceEditsDirs_WithoutSharedEditsIncluded
    // 1. You need to use the HDFS 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void test_getNamespaceEditsDirs_WithoutSharedEditsIncluded() {
        // Prepare the configuration with dfs.namenode.shared.edits.dir set to a valid URI
        Configuration conf = new Configuration();
        // Obtain the configuration key dynamically for dfs.namenode.shared.edits.dir
        String sharedEditsDirKey = DFSConfigKeys.DFS_NAMENODE_SHARED_EDITS_DIR_KEY;
        conf.set(sharedEditsDirKey, "file:///shared/edits/dir");

        try {
            // Call the method with includeShared set to false
            List<URI> editsDirs = FSNamesystem.getNamespaceEditsDirs(conf, false);
            
            // Assert that the returned list does not include the shared edits directory configured in dfs.namenode.shared.edits.dir
            URI sharedEditsUri = new URI(conf.getTrimmed(sharedEditsDirKey));
            for (URI dir : editsDirs) {
                assert !dir.equals(sharedEditsUri) : "Shared edits directory was included in the result, but it should not be.";
            }
        } catch (Exception e) {
            e.printStackTrace();
            assert false : "An exception occurred during the test: " + e.getMessage();
        }
    }
}