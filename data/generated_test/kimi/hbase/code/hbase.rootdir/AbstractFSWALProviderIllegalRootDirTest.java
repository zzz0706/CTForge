package org.apache.hadoop.hbase.wal;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.apache.hadoop.hbase.testclassification.SmallTests;

import java.io.IOException;

@Category(SmallTests.class)
public class AbstractFSWALProviderIllegalRootDirTest {

    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
            HBaseClassTestRule.forClass(AbstractFSWALProviderIllegalRootDirTest.class);

    @Test(expected = IllegalArgumentException.class)
    public void testIllegalArgumentExceptionThrownWhenRootDirMissing() throws IOException {
        // 1. Create a Configuration instance
        Configuration conf = new Configuration();

        // 2. Explicitly unset hbase.rootdir
        conf.unset(HConstants.HBASE_DIR);

        // 3. Invoke the method under test
        AbstractFSWALProvider.getServerNameFromWALDirectoryName(conf, "/some/path");
    }
}