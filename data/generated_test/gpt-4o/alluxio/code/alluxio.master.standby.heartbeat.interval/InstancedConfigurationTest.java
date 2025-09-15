package alluxio.conf;

import alluxio.conf.AlluxioProperties;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.*;

/**
 * Unit test for InstancedConfiguration.
 */
public class InstancedConfigurationTest {

    private InstancedConfiguration instancedConfiguration;

    @Before
    public void setUp() {
        // Prepare AlluxioProperties with mocked configuration values
        AlluxioProperties properties = new AlluxioProperties();
        properties.set(PropertyKey.MASTER_STANDBY_HEARTBEAT_INTERVAL, "2000"); // 2 seconds
        properties.set(PropertyKey.MASTER_HEARTBEAT_TIMEOUT, "1000"); // 1 second

        // Create InstancedConfiguration using mocked properties
        instancedConfiguration = new InstancedConfiguration(properties);
    }

    @Test(expected = IllegalStateException.class)
    public void testValidateThrowsExceptionForInvalidHeartbeatConfiguration() {
        // Prepare a scenario where the interval is greater than or equal to the timeout
        instancedConfiguration.set(PropertyKey.MASTER_STANDBY_HEARTBEAT_INTERVAL, "2000"); // 2 seconds
        instancedConfiguration.set(PropertyKey.MASTER_HEARTBEAT_TIMEOUT, "1000");          // 1 second
        
        // Trigger the configuration validation logic
        instancedConfiguration.validate();

        // Since validation logic should throw an exception, the test will pass if an IllegalStateException is thrown.
    }
}