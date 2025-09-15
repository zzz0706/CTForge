package alluxio.conf;

import alluxio.conf.PropertyKey;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import org.junit.Test;

import static org.junit.Assert.*;

public class ConfigurationValidationTest {

    /**
     * Test case to validate the configuration "alluxio.jvm.monitor.warn.threshold"
     * and check if its value satisfies constraints and dependencies.
     */
    @Test
    public void testJvmMonitorWarnThresholdConfigValidation() {
        // Step 1: Prepare the Alluxio configuration instance correctly
        AlluxioConfiguration configuration = InstancedConfiguration.defaults();

        // Step 2: Read the configuration value using the Alluxio 2.1.0 API
        String configValue = configuration.get(PropertyKey.JVM_MONITOR_WARN_THRESHOLD_MS);

        // Step 3: Validate the configuration constraints and dependencies
        // Constraint: Ensure the value is not empty and follows the expected format
        assertNotNull("The configuration value for alluxio.jvm.monitor.warn.threshold must not be null.", configValue);
        assertFalse("The configuration value for alluxio.jvm.monitor.warn.threshold must not be empty.", configValue.isEmpty());

        // Ensure the value can be parsed into a time duration in milliseconds
        long warnThresholdMs;
        try {
            warnThresholdMs = configuration.getMs(PropertyKey.JVM_MONITOR_WARN_THRESHOLD_MS);
        } catch (IllegalArgumentException e) {
            fail("The configuration value for alluxio.jvm.monitor.warn.threshold is invalid. Ensure it follows the correct format, e.g., '10sec'.");
            return; // Additional safety precaution in case fail does not terminate execution
        }

        // Additional constraints
        // 1. The warn threshold cannot be negative or zero as it defines a time duration
        assertTrue("The configuration value for alluxio.jvm.monitor.warn.threshold must be greater than zero.", warnThresholdMs > 0);

        // Step 4: Check if the configuration is consistent with related configurations if applicable
        // Dependencies: Configuration may be affected by MASTER_JVM_MONITOR_ENABLED or WORKER_JVM_MONITOR_ENABLED
        boolean masterJvmMonitorEnabled = configuration.getBoolean(PropertyKey.MASTER_JVM_MONITOR_ENABLED);
        boolean workerJvmMonitorEnabled = configuration.getBoolean(PropertyKey.WORKER_JVM_MONITOR_ENABLED);

        if (!masterJvmMonitorEnabled && !workerJvmMonitorEnabled) {
            warn("The configuration 'alluxio.jvm.monitor.warn.threshold' has no effect because both MASTER_JVM_MONITOR_ENABLED and WORKER_JVM_MONITOR_ENABLED are disabled.");
        }

        // If other threshold configurations are set, ensure dependencies are consistent
        long infoThresholdMs = configuration.getMs(PropertyKey.JVM_MONITOR_INFO_THRESHOLD_MS);
        assertTrue("INFO threshold must be less than or equal to WARN threshold.", infoThresholdMs <= warnThresholdMs);
    }

    /**
     * Helper method to log warnings during unit test execution.
     */
    private void warn(String message) {
        System.out.println("[WARN] " + message);
    }
}