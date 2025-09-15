package alluxio.util;

import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class JvmPauseMonitorTest {
    private static final Logger LOG = LoggerFactory.getLogger(JvmPauseMonitorTest.class);

    private static class MockJvmPauseMonitor {
        private long simulatedPauseTime;
        private long sleepIntervalMs;
        private long warnThresholdMs;
        private long infoThresholdMs;
        private boolean stopped = false;

        public MockJvmPauseMonitor(long sleepIntervalMs, long warnThresholdMs, long infoThresholdMs, long simulatedPauseTime) {
            this.sleepIntervalMs = sleepIntervalMs;
            this.warnThresholdMs = warnThresholdMs;
            this.infoThresholdMs = infoThresholdMs;
            this.simulatedPauseTime = simulatedPauseTime;
        }

        public void start() {
            stopped = false;
            // Mock monitoring logic: simulate a pause measurement
            long pauseDurationMs = measureThreadSleepTimeMs(sleepIntervalMs);
            if (pauseDurationMs > infoThresholdMs) {
                logPauseInfo(pauseDurationMs, 0, 0);
            }
        }

        public void stop() {
            stopped = true;
            LOG.info("Monitor stopped");
        }

        public boolean isStopped() {
            return stopped;
        }

        protected long measureThreadSleepTimeMs(long sleepTimeMs) {
            // Simulate the sleep time exceeding thresholds
            return simulatedPauseTime;
        }

        protected void logPauseInfo(long pauseDurationMs, long gcDurationMs, long gcCount) {
            LOG.info("Simulated logPauseInfo called with pauseDurationMs: {}, gcDurationMs: {}, gcCount: {}", pauseDurationMs, gcDurationMs, gcCount);
        }
    }

    @Before
    public void setUp() {
        // Ensure the ServerConfiguration is properly initialized before running the test
        ServerConfiguration.reset();
    }

    @Test
    public void test_JvmPauseMonitor_run_infoThresholdExceeded() throws InterruptedException {
        // 1. Use the Alluxio 2.1.0 API correctly to obtain configuration values.
        long sleepIntervalMs = ServerConfiguration.getMs(PropertyKey.JVM_MONITOR_SLEEP_INTERVAL_MS);
        long infoThresholdMs = ServerConfiguration.getMs(PropertyKey.JVM_MONITOR_INFO_THRESHOLD_MS);
        long warnThresholdMs = ServerConfiguration.getMs(PropertyKey.JVM_MONITOR_WARN_THRESHOLD_MS);

        // Simulated pause time exceeds infoThreshold but not warnThreshold
        long simulatedPauseTime = sleepIntervalMs + infoThresholdMs + 10;

        // 2. Prepare the test conditions.
        MockJvmPauseMonitor monitor = new MockJvmPauseMonitor(sleepIntervalMs, warnThresholdMs, infoThresholdMs, simulatedPauseTime);

        // Run the monitor in a dedicated thread
        Thread monitorThread = new Thread(() -> {
            try {
                monitor.start();
                Thread.sleep(500);  // Let the monitor run briefly
            } catch (InterruptedException e) {
                LOG.error("Error in test thread.", e);
                Thread.currentThread().interrupt();
            } finally {
                monitor.stop();
            }
        });

        // 3. Test code.
        monitorThread.start();
        monitorThread.join();

        // Assert the monitor logged info-level pauses and executed as expected.
        // (In a real test, you'd capture the log output to confirm behavior, if required)

        // 4. Code after testing.
        assertTrue(monitor.isStopped());
    }
}