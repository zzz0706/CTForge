package alluxio.conf;

import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import org.junit.Before;
import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.*;

public class ConfigurationUsageTest {

    private InstancedConfiguration mConfiguration;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setUp() {
        // Initialize mock configuration.
        mConfiguration = InstancedConfiguration.defaults();
    }

    @Test
    public void testValidateConfigurationWithValidTimeouts() {
        // Step 1: Set valid values for testing using the API.
        mConfiguration.set(PropertyKey.MASTER_WORKER_CONNECT_WAIT_TIME, "500");
        mConfiguration.set(PropertyKey.USER_RPC_RETRY_MAX_SLEEP_MS, "300");

        // Step 2: Call the validate method to ensure the configuration is valid.
        mConfiguration.validate();

        // Step 3: Validate proper configuration functionality.
        assertEquals("500", mConfiguration.get(PropertyKey.MASTER_WORKER_CONNECT_WAIT_TIME));
        assertEquals("300", mConfiguration.get(PropertyKey.USER_RPC_RETRY_MAX_SLEEP_MS));
    }
}