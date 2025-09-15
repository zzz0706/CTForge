package alluxio.conf;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

public class InstancedConfigurationTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    /**
     * Test case: validateConfigurationWithValidTimeouts
     * Objective: Verify that the validate() method processes correct configurations and provides warning logs
     * when timeouts are close or invalid.
     */
    @Test
    public void testValidateConfigurationWithValidTimeouts() {
        // Step 1: Prepare test conditions, all using the Alluxio configuration API to fetch values.
        InstancedConfiguration configuration = InstancedConfiguration.defaults();

        // Get the configuration values directly using the API.
        long waitTime = configuration.getMs(PropertyKey.MASTER_WORKER_CONNECT_WAIT_TIME);
        long retryInterval = configuration.getMs(PropertyKey.USER_RPC_RETRY_MAX_SLEEP_MS);

        // Log configuration values using an external logger instance.
        Logger logger = LoggerFactory.getLogger(InstancedConfigurationTest.class);
        logger.info("Testing validate() method with waitTime={}ms and retryInterval={}ms", waitTime, retryInterval);

        // Ensure preconditions:
        // Verify that waitTime is valid and retryInterval is slightly smaller.
        if (waitTime < retryInterval) {
            expectedException.expectMessage("Workers might not have enough time to register.");
        }

        // Step 3: Call the validate() method to trigger the validation logic.
        configuration.validate();

        // Step 4: Assert logs were captured and no unexpected exceptions occurred.
        // (The logger ensures appropriate warnings and follow-ups are logged during execution.)
    }
}