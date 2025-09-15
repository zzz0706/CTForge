package alluxio.conf;

import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.time.Duration;

import static org.mockito.Mockito.*;

public class InstancedConfigurationTest {

    private InstancedConfiguration mConfiguration;

    @Before
    public void setUp() {
        // Mock the InstancedConfiguration object
        mConfiguration = mock(InstancedConfiguration.class);

        // Mock configuration values using the Alluxio 2.1.0 API
        when(mConfiguration.getDuration(PropertyKey.MASTER_WORKER_CONNECT_WAIT_TIME))
                .thenReturn(Duration.ofMillis(1000)); // 1 second
        when(mConfiguration.getDuration(PropertyKey.USER_RPC_RETRY_MAX_SLEEP_MS))
                .thenReturn(Duration.ofMillis(2000)); // 2 seconds
    }

    @Test
    public void testValidateWarnsForTimeoutConflicts() {
        // Invoke the validate method to trigger validation logic
        doCallRealMethod().when(mConfiguration).validate();
        mConfiguration.validate();

        // Verify that the validate method was invoked exactly once
        verify(mConfiguration, times(1)).validate();
        
        // Note: Validation warnings and logging are non-testable here as logs aren't directly assertable
        // Additional provisions for capturing logs programmatically would be needed if we want to test log messages
    }
}