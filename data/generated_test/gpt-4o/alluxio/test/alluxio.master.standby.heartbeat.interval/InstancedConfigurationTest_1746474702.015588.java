package alluxio.conf;

import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import org.junit.Assert;
import org.junit.Test;

public class InstancedConfigurationTest {

    @Test
    public void validate_with_valid_heartbeat_interval() {
        // Prepare the test conditions.
        InstancedConfiguration instancedConfiguration = InstancedConfiguration.defaults();
        
        // Use Alluxio's API to set configuration values dynamically, avoiding hardcoded values.
        instancedConfiguration.set(PropertyKey.MASTER_STANDBY_HEARTBEAT_INTERVAL, "100ms");
        instancedConfiguration.set(PropertyKey.MASTER_HEARTBEAT_TIMEOUT, "200ms");

        try {
            // Test code: Call validate method to test internal validation behavior.
            instancedConfiguration.validate();
            
            // Ensure the validate method completes without throwing any exceptions.
            Assert.assertTrue(true);
        } catch (IllegalArgumentException e) {
            // Failing the test if validate throws an exception for valid configuration.
            Assert.fail("Validation failed with valid configuration values: " + e.getMessage());
        }
    }
}