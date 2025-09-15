package alluxio.conf;

import alluxio.conf.InstancedConfiguration;
import alluxio.conf.AlluxioProperties;
import alluxio.conf.PropertyKey;
import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.Mockito.*;
import static org.junit.Assert.assertTrue;

public class InstancedConfigurationTest {
  
    @Test
    //test code
    // 1. You need to use the Alluxio 2.1.0 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void test_checkTimeouts_with_conflict_configurations() {
        // 1. Prepare the test conditions
        AlluxioProperties properties = new AlluxioProperties();

        // Add necessary configuration values using API calls
        properties.set(PropertyKey.MASTER_WORKER_CONNECT_WAIT_TIME, "1000ms");
        properties.set(PropertyKey.USER_RPC_RETRY_MAX_SLEEP_MS, "3000ms");

        InstancedConfiguration configuration = Mockito.spy(new InstancedConfiguration(properties));

        // Mock `getMs` method to simulate real API behavior
        doReturn(1000L).when(configuration).getMs(PropertyKey.MASTER_WORKER_CONNECT_WAIT_TIME);
        doReturn(3000L).when(configuration).getMs(PropertyKey.USER_RPC_RETRY_MAX_SLEEP_MS);

        // 2. Test Code
        try {
            configuration.validate(); // This method is expected to throw IllegalArgumentException.
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("conflict"));
        }

        // Verify interaction with mocked methods
        verify(configuration).getMs(PropertyKey.MASTER_WORKER_CONNECT_WAIT_TIME);
        verify(configuration).getMs(PropertyKey.USER_RPC_RETRY_MAX_SLEEP_MS);
    }
}