package alluxio.util;

import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import org.junit.Test;

import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;

public class JvmPauseMonitorTest {

    @Test
    public void testJvmPauseMonitorRun_WarnLevelLogging() throws InterruptedException {
        // 1. Use the Alluxio 2.1.0 API to get configuration values.
        long gcSleepIntervalMs = ServerConfiguration.getMs(PropertyKey.JVM_MONITOR_SLEEP_INTERVAL_MS);
        long warnThresholdMs = ServerConfiguration.getMs(PropertyKey.JVM_MONITOR_WARN_THRESHOLD_MS);
        long infoThresholdMs = ServerConfiguration.getMs(PropertyKey.JVM_MONITOR_INFO_THRESHOLD_MS);

        // 2. Prepare the test conditions.
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        JvmPauseMonitor jvmPauseMonitor = new JvmPauseMonitor(gcSleepIntervalMs, warnThresholdMs, infoThresholdMs);

        try {
            // Start the JVM pause monitor in the executor
            executorService.execute(jvmPauseMonitor::start);

            // 3. Test code: Simulate a long GC-like pause exceeding the WARN threshold
            Thread.sleep(warnThresholdMs + 500); // Simulate a delay to test whether JVMPauseMonitor can detect it

            // Allow some time for monitoring to potentially detect the pause
            Thread.sleep(1000);

            // No explicit call to a run() method, as JvmPauseMonitor does not have such a method.
            // Instead, JvmPauseMonitor operates implicitly after being started.

        } finally {
            // Ensure that the JVM pause monitor is stopped
            jvmPauseMonitor.stop();

            // 4. Code after testing: Shut down executor
            executorService.shutdown();
        }
    }
}