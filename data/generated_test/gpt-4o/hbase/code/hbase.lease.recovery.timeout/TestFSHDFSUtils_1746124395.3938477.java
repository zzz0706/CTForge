package org.apache.hadoop.hbase.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.util.EnvironmentEdge;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.CancelableProgressable;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.ClassRule;
import org.mockito.Mockito;

import java.io.IOException;

import static org.junit.Assert.assertFalse;

@Category({MediumTests.class, ClientTests.class})
public class TestFSHDFSUtils {

    @ClassRule // HBaseClassTestRule ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
            HBaseClassTestRule.forClass(TestFSHDFSUtils.class);

    private Configuration conf;
    private DistributedFileSystem dfsMock;
    private CancelableProgressable reporterMock;
    private Path pathMock;

    @Before
    public void setUp() {
        // Mock setup
        conf = new Configuration();
        dfsMock = Mockito.mock(DistributedFileSystem.class);
        reporterMock = Mockito.mock(CancelableProgressable.class);
        pathMock = Mockito.mock(Path.class);

        // Inject a valid instance of the custom edge for testing
        EnvironmentEdge customEdge = new EnvironmentEdge() {
            @Override
            public long currentTime() {
                return System.currentTimeMillis();
            }
        };
        EnvironmentEdgeManager.injectEdge(customEdge);
    }

    @Test
    public void testCheckIfTimedoutWithinTimeout() throws IOException {
        // 1. Use the hbase 2.2.2 API to fetch configuration values.
        int configuredTimeout = conf.getInt("hbase.lease.recovery.timeout", 900000); // Default is 900,000 ms

        // 2. Prepare simulation of EnvironmentEdgeManager time behavior
        long startWaiting = EnvironmentEdgeManager.currentTime();
        long recoveryTimeout = configuredTimeout + startWaiting;

        EnvironmentEdge customEdge = new EnvironmentEdge() {
            @Override
            public long currentTime() {
                return startWaiting + 100_000; // Simulate 100 seconds later
            }
        };
        EnvironmentEdgeManager.injectEdge(customEdge);

        // 3. Call the corrected `checkIfTimedout` method
        boolean isTimedOut = new FSHDFSUtils().checkIfTimedout(conf, recoveryTimeout, 0, pathMock, startWaiting);

        // 4. Validate the outcome of the test
        assertFalse("Lease recovery should not timeout within the configured recovery period", isTimedOut);
    }
}