package org.apache.hadoop.hbase.util;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.ClassRule;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hbase.util.CancelableProgressable;

import java.io.IOException;

import static org.mockito.Mockito.*;
import static org.junit.Assert.assertFalse;

@Category(SmallTests.class)
public class TestFSHDFSUtils {

    @ClassRule // HBaseClassTestRule ClassRule
    public static final HBaseClassTestRule CLASS_RULE = HBaseClassTestRule.forClass(TestFSHDFSUtils.class);

    @Test
    public void testRecoverDFSFileLeaseTimeoutExceeded() throws IOException {
        // Test code
        // 1. Correctly obtain configuration values using HBase 2.2.2 API instead of hardcoding.
        Configuration conf = new Configuration();
        conf.setLong("hbase.lease.recovery.timeout", 900000); // Default value for lease recovery timeout
        long recoveryTimeout = conf.getLong("hbase.lease.recovery.timeout", 900000);

        // 2. Prepare the test conditions: Mock the DistributedFileSystem instance.
        DistributedFileSystem dfs = mock(DistributedFileSystem.class);
        when(dfs.recoverLease(any(Path.class))).thenReturn(false);

        // Create test path instance and mock CancelableProgressable (for progress reporting).
        Path testPath = new Path("/test/path");
        CancelableProgressable reporter = mock(CancelableProgressable.class);

        // Spy on utility class to observe the control flow.
        FSHDFSUtils utilSpy = spy(FSHDFSUtils.class);

        // 3. Test recovery behavior.
        try {
            boolean recovered = utilSpy.recoverDFSFileLease(dfs, testPath, conf, reporter);

            // 4. Use assertions provided by JUnit to verify the expected behavior.
            // Since the timeout was exceeded, recovery should fail.
            assertFalse("Expected lease recovery to fail after exceeding the timeout period.", recovered);

            // Verify that the `checkIfTimedout` method was invoked.
            verify(utilSpy).checkIfTimedout(eq(conf), eq(recoveryTimeout), anyInt(), eq(testPath), anyLong());
        } catch (IOException e) {
            // Handle the specific exception thrown during testing
            System.err.println("Test failed due to IOException: " + e.getMessage());
        }
    }
}