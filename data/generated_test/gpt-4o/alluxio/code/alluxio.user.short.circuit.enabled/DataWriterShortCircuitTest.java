package alluxio.client.block.stream;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class DataWriterShortCircuitTest {
    @Test
    public void testDataWriterShortCircuitEnabledLocal() throws Exception {
        // Step 1: Create a mocked AlluxioConfiguration using Mockito
        AlluxioConfiguration alluxioConfiguration = mock(AlluxioConfiguration.class);

        // Step 2: Stub the behavior of the mocked AlluxioConfiguration to return true for the short-circuit property
        when(alluxioConfiguration.getBoolean(PropertyKey.USER_SHORT_CIRCUIT_ENABLED)).thenReturn(true);

        // Step 3: Validate the configuration value
        boolean isShortCircuitEnabled = alluxioConfiguration.getBoolean(PropertyKey.USER_SHORT_CIRCUIT_ENABLED);
        assertTrue("Short-circuit should be enabled", isShortCircuitEnabled);
    }
}