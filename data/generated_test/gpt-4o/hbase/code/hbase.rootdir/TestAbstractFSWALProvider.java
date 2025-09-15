package org.apache.hadoop.hbase.wal;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

@Category(SmallTests.class)
public class TestAbstractFSWALProvider {

    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
            HBaseClassTestRule.forClass(TestAbstractFSWALProvider.class);

    @Test
    public void testGetServerNameFromWALDirectoryName_withInvalidRootDirPath() throws Exception {
        // 1. You need to use the hbase 2.2.2 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        HBaseTestingUtility testingUtility = new HBaseTestingUtility();
        Configuration conf = testingUtility.getConfiguration();

        // 2. Prepare the test conditions: invalid path that does not match the expected WAL directory structure.
        String invalidPath = "/invalid/path/for/wal";

        // 3. Test code: Invoke method with configuration and invalid path.
        ServerName result = AbstractFSWALProvider.getServerNameFromWALDirectoryName(conf, invalidPath);

        // 4. Code after testing: Validate the output. Result should be null as the path is invalid.
        assertNull("Expected result to be null for invalid path", result);
    }
}