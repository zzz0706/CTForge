package org.apache.hadoop.hbase.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.util.FSHDFSUtils;
import org.apache.hadoop.hbase.util.CancelableProgressable;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.ClassRule;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

@Category({SmallTests.class, ClientTests.class})
public class TestFSHDFSUtils {

    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
            HBaseClassTestRule.forClass(TestFSHDFSUtils.class);

    @Test
    public void testRecoverDFSFileLease_FirstAttemptSuccessful() throws Exception {
        // 1. Correctly use the HBase 2.2.2 API to handle configurations.
        // Create a Configuration object and set the timeout for lease recovery.
        Configuration config = new Configuration();
        config.setLong("hbase.lease.recovery.dfs.timeout", 64 * 1000);

        // 2. Prepare the test conditions.
        // Create a mock instance of DistributedFileSystem (dfs).
        DistributedFileSystem mockDFS = mock(DistributedFileSystem.class);
        // Mock the `recoverLease` logic to return true on the first attempt.
        when(mockDFS.recoverLease(any(Path.class))).thenReturn(true);

        // Create a mock Path object pointing to a valid HDFS path.
        Path mockPath = new Path("/test-hdfs-path");

        // Create a CancelableProgressable instance that does nothing on cancellation.
        CancelableProgressable mockReporter = mock(CancelableProgressable.class);

        // 3. Test code.
        // Create an instance of FSHDFSUtils to invoke the non-static method.
        FSHDFSUtils fsHdfsUtils = new FSHDFSUtils();
        boolean result = fsHdfsUtils.recoverDFSFileLease(mockDFS, mockPath, config, mockReporter);

        // 4. Code after testing.
        // Validate that the behavior is correct.
        assertTrue("The lease recovery should succeed on the first attempt.", result);
        verify(mockDFS, times(1)).recoverLease(any(Path.class));
    }
}