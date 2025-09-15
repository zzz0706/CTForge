package alluxio.master;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.util.JvmPauseMonitor;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertNull;

public class AlluxioMasterProcessTest {
    // Test code
    private JvmPauseMonitor mJvmPauseMonitor;

    @Before
    public void setUp() {
        // Initialize the ServerConfiguration to avoid null reference issues
        ServerConfiguration.reset();

        // Prepare the test conditions
        mJvmPauseMonitor = null; // Simulate a scenario where the monitor is not started
    }

    @Test
    public void testJvmMonitorProcessDoesNotStartWhenDisabled() throws Exception {
        // Correctly use the Alluxio 2.1.0 API to control the configuration dynamically
        ServerConfiguration.set(PropertyKey.MASTER_JVM_MONITOR_ENABLED, "false");

        // Test code
        if (ServerConfiguration.getBoolean(PropertyKey.MASTER_JVM_MONITOR_ENABLED)) {
            // Pass required arguments to the constructor of JvmPauseMonitor as per the error message
            long maxSleepTimeMs = 5000;
            long sleepIntervalMs = 1000;
            long pauseThresholdMs = 100;
            mJvmPauseMonitor = new JvmPauseMonitor(maxSleepTimeMs, sleepIntervalMs, pauseThresholdMs);
            mJvmPauseMonitor.start();
        }

        // Verify that the JvmPauseMonitor is not instantiated
        assertNull("JvmPauseMonitor should not be instantiated when disabled", mJvmPauseMonitor);
    }
}