package alluxio.conf;

import alluxio.conf.AlluxioProperties;
import alluxio.conf.PropertyKey;
import alluxio.conf.InstancedConfiguration;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ConfigurationValidationTest {

    @Test
    public void testNetworkConnectionHealthCheckTimeoutConfiguration() {
        /*
         * Step 1: Prepare the test conditions.
         * Step 2: Use Alluxio 2.1.0 API correctly to obtain configuration values.
         * Step 3: Test the configuration value against constraints.
         * Step 4: Handle any cleanup or conclusion of the test.
         */

        try {
            // Step 1: Prepare the test conditions
            AlluxioProperties properties = new AlluxioProperties();

            // Instantiate a correct configuration object
            InstancedConfiguration configuration = new InstancedConfiguration(properties);
            
            // Step 2: Use correct Alluxio API to obtain configuration values
            String healthCheckTimeoutStr = configuration.get(PropertyKey.NETWORK_CONNECTION_HEALTH_CHECK_TIMEOUT);

            // Step 3: Constraints validation
            // 1. Ensure the value is not null or empty
            assertTrue("Configuration value should not be null or empty",
                    healthCheckTimeoutStr != null && !healthCheckTimeoutStr.isEmpty());

            // 2. Ensure the value can be parsed as a valid time duration
            long healthCheckTimeoutMs;
            try {
                healthCheckTimeoutMs = configuration.getMs(PropertyKey.NETWORK_CONNECTION_HEALTH_CHECK_TIMEOUT);
            } catch (Exception e) {
                fail("Invalid configuration value: Unable to parse "
                        + healthCheckTimeoutStr + " to a valid duration in milliseconds");
                return;
            }

            // 3. Ensure the value is within acceptable range (>0)
            assertTrue("Health check timeout must be greater than zero",
                    healthCheckTimeoutMs > 0);

            // 4. Ensure the value does not exceed maximum allowed limit
            long maxTimeoutMs = TimeUnit.MINUTES.toMillis(5);
            assertTrue("Health check timeout must not exceed " + maxTimeoutMs + " milliseconds",
                    healthCheckTimeoutMs <= maxTimeoutMs);

        } catch (Exception e) {
            fail("Exception occurred during validation of "
                    + PropertyKey.NETWORK_CONNECTION_HEALTH_CHECK_TIMEOUT + ": " + e.getMessage());
        }

        // Step 4: Clean up or conclude test execution
        System.out.println("Validation completed for "
                + PropertyKey.NETWORK_CONNECTION_HEALTH_CHECK_TIMEOUT);
    }
}