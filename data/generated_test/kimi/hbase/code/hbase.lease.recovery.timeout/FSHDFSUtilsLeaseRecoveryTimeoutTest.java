package org.apache.hadoop.hbase.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.*;

@Category({MiscTests.class, SmallTests.class})
public class FSHDFSUtilsLeaseRecoveryTimeoutTest {

    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
        HBaseClassTestRule.forClass(FSHDFSUtilsLeaseRecoveryTimeoutTest.class);

    @Test
    public void testLeaseRecoveryTimeoutIsReadFromConfiguration() throws Exception {
        // 1. You need to use the hbase 2.2.2 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        Configuration conf = new Configuration();
        conf.setInt("hbase.lease.recovery.timeout", 123456);

        // 2. Prepare the test conditions.
        long expectedTimeout = conf.getInt("hbase.lease.recovery.timeout", 900000);
        Path path = new Path("/test/path");

        DistributedFileSystem mockDfs = mock(DistributedFileSystem.class);
        when(mockDfs.recoverLease(path)).thenReturn(false);

        // 3. Test code.
        boolean recovered = new FSHDFSUtils().recoverDFSFileLease(mockDfs, path, conf, null);

        // 4. Code after testing.
        assertFalse("Expected lease recovery to timeout", recovered);
    }
}