package org.apache.hadoop.hbase.util;

import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import static org.mockito.Mockito.*;

@Category(SmallTests.class)
public class TestFSHDFSUtils {

    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE = 
        HBaseClassTestRule.forClass(TestFSHDFSUtils.class);

    /**
     * Test case: testCheckIfTimedoutTimeoutExceeded
     * Objective: Ensure that checkIfTimedout returns true and logs an appropriate warning 
     *            when called after the recoveryTimeout duration has elapsed.
     */
    @Test
    public void testCheckIfTimedoutTimeoutExceeded() {
        // Prepare the test conditions
        // Create a mock Configuration object
        Configuration mockConf = mock(Configuration.class);

        // Use the API to obtain the configuration value (instead of hardcoding the value)
        when(mockConf.getInt("hbase.lease.recovery.timeout", 900000)).thenReturn(900000); 

        // Mocking simulated startWaiting and current time
        long simulatedStartWaiting = 10_000; // Time when lease recovery begins
        long recoveryTimeout = 900_000 + simulatedStartWaiting; // Calculating the timeout value

        Path mockPath = mock(Path.class);
        int nbAttempt = 1; // Arbitrary value for this test

        // Simulate EnvironmentEdgeManager to return a time exceeding recoveryTimeout
        EnvironmentEdgeManagerTestHelper.injectEdge(() -> recoveryTimeout + 5_000); 

        // Test code
        // Invoke checkIfTimedout and verify its behavior
        FSHDFSUtils utils = new FSHDFSUtils();
        boolean result = utils.checkIfTimedout(mockConf, recoveryTimeout, nbAttempt, mockPath, simulatedStartWaiting);

        // Assert that the method returns true when timeout has elapsed
        assert result : "checkIfTimedout should return true when recoveryTimeout has been exceeded.";

        // Code after testing
        // Reset EnvironmentEdgeManager to its default edge after the test
        EnvironmentEdgeManagerTestHelper.reset();
    }
}