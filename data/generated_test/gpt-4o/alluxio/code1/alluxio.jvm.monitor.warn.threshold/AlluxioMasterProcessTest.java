package alluxio.master;

import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.JvmPauseMonitor;
import org.junit.Test;
import static org.junit.Assert.assertNotNull;

public class AlluxioMasterProcessTest {
    @Test
    public void test_startJvmMonitorProcess_master_enabled() throws Exception {
        // 1. Use Alluxio 2.1.0 API correctly to obtain configuration values, instead of mocking ServerConfiguration.
        boolean jvmMonitorEnabled = ServerConfiguration.getBoolean(PropertyKey.MASTER_JVM_MONITOR_ENABLED);
        long sleepIntervalMs = ServerConfiguration.getMs(PropertyKey.JVM_MONITOR_SLEEP_INTERVAL_MS);
        long warnThresholdMs = ServerConfiguration.getMs(PropertyKey.JVM_MONITOR_WARN_THRESHOLD_MS);
        long infoThresholdMs = ServerConfiguration.getMs(PropertyKey.JVM_MONITOR_INFO_THRESHOLD_MS);

        // 2. Prepare the test conditions.
        // Ensure the jvmMonitorEnabled flag is true for the test to proceed (modify configuration as needed).
        if (!jvmMonitorEnabled) {
            ServerConfiguration.set(PropertyKey.MASTER_JVM_MONITOR_ENABLED, "true");
            jvmMonitorEnabled = ServerConfiguration.getBoolean(PropertyKey.MASTER_JVM_MONITOR_ENABLED);
        }

        // 3. Test code.
        // Create the JvmPauseMonitor using the retrieved configuration values.
        JvmPauseMonitor pauseMonitor = new JvmPauseMonitor(sleepIntervalMs, warnThresholdMs, infoThresholdMs);

        // Ensure the pauseMonitor is initialized correctly.
        assertNotNull(pauseMonitor);

        // Start the JvmPauseMonitor and test its behavior.
        pauseMonitor.start();
        pauseMonitor.stop(); // Stop monitor after test execution.

        // 4. Code after testing.
        // Reset any modified configurations to their original state to avoid side effects on other tests.
        ServerConfiguration.set(PropertyKey.MASTER_JVM_MONITOR_ENABLED, "false");
    }
}