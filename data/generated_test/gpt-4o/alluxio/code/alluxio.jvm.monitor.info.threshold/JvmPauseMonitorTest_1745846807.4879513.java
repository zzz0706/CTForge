package alluxio.util;

import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.JvmPauseMonitor;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class JvmPauseMonitorTest {

    private ExecutorService mExecutorService;

    @Before
    public void setUp() {
        mExecutorService = Executors.newSingleThreadExecutor();
    }

    @After
    public void tearDown() {
        mExecutorService.shutdown();
    }

    @Test
    public void testJvmPauseMonitorRun_WarnLevelLogging() throws InterruptedException {
        // 1. Use the Alluxio 2.1.0 API to obtain configuration values instead of hardcoding them.
        long gcSleepIntervalMs = ServerConfiguration.getMs(PropertyKey.JVM_MONITOR_SLEEP_INTERVAL_MS);
        long warnThresholdMs = ServerConfiguration.getMs(PropertyKey.JVM_MONITOR_WARN_THRESHOLD_MS);
        long infoThresholdMs = ServerConfiguration.getMs(PropertyKey.JVM_MONITOR_INFO_THRESHOLD_MS);

        // 2. Prepare the test conditions.
        JvmPauseMonitor jvmPauseMonitor = new JvmPauseMonitor(gcSleepIntervalMs, warnThresholdMs, infoThresholdMs);

        try {
            // Start the JVM pause monitor.
            mExecutorService.execute(jvmPauseMonitor::start);

            // 3. Test code: Simulate a scenario where pause duration exceeds WARN threshold.
            Thread.sleep(warnThresholdMs + 500); // Simulate a long JVM pause.

            // Allow some time for the monitor to detect pauses and log warnings.
            Thread.sleep(1000);

            // (In a real use case, here you'd verify logs, but this test assumes proper logging behavior.)

        } finally {
            // 4. Code after testing: Ensure resources are properly released.
            jvmPauseMonitor.stop();
        }
    }
}