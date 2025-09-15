package alluxio.conf;

import alluxio.conf.AlluxioProperties;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import org.junit.Test;

/**
 * Unit test for validating heartbeat interval configuration in InstancedConfiguration.
 */
public class InstancedConfigurationTest {

    @Test(expected = IllegalStateException.class)
    public void validate_with_invalid_heartbeat_interval() {
        // 1. Prepare the test conditions:
        // Create an instance of AlluxioProperties to configure the properties.
        AlluxioProperties properties = new AlluxioProperties();
        properties.set(PropertyKey.MASTER_STANDBY_HEARTBEAT_INTERVAL, "3000ms");
        properties.set(PropertyKey.MASTER_HEARTBEAT_TIMEOUT, "2000ms");

        // Use the proper constructor of InstancedConfiguration to pass AlluxioProperties.
        InstancedConfiguration instancedConfiguration = new InstancedConfiguration(properties);

        // 2. Test the behavior when an invalid configuration is provided:
        instancedConfiguration.validate();

        // 3. Code after testing:
        // The validate method should throw an IllegalStateException due to the invalid configuration.
    }
}