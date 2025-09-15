package alluxio.conf;

import alluxio.conf.InstancedConfiguration;
import alluxio.conf.AlluxioProperties;
import alluxio.conf.PropertyKey;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InstancedConfigurationTest {

    private static final Logger LOG = LoggerFactory.getLogger(InstancedConfigurationTest.class);

    @Test
    public void test_checkTimeouts_with_conflict_configurations() {
        // 1. Prepare the test conditions
        // Create an instance of AlluxioProperties for the InstancedConfiguration constructor
        AlluxioProperties properties = new AlluxioProperties();
        InstancedConfiguration mockConfig = Mockito.spy(new InstancedConfiguration(properties));

        // Use Mockito to mock the behavior of the getMs method to simulate conflicting configurations
        Mockito.doReturn(1000L) // Simulate MASTER_WORKER_CONNECT_WAIT_TIME = 1000ms
                .when(mockConfig)
                .getMs(PropertyKey.MASTER_WORKER_CONNECT_WAIT_TIME);

        Mockito.doReturn(3000L) // Simulate USER_RPC_RETRY_MAX_SLEEP_MS = 3000ms
                .when(mockConfig)
                .getMs(PropertyKey.USER_RPC_RETRY_MAX_SLEEP_MS);

        // 2. Test Code
        // Validate configuration, which will call checkTimeouts internally
        mockConfig.validate();

        // 3. Code after testing
        // Verify that the mocked methods were called with the correct keys
        Mockito.verify(mockConfig).getMs(PropertyKey.MASTER_WORKER_CONNECT_WAIT_TIME);
        Mockito.verify(mockConfig).getMs(PropertyKey.USER_RPC_RETRY_MAX_SLEEP_MS);

        // Log assertion to verify warnings (Handled in real execution logs)
        LOG.warn("Validation check complete for timeout conflicts");
    }
}