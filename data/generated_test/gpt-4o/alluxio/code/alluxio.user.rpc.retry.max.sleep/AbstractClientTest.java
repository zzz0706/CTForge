package alluxio.client;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.AbstractClient;
import alluxio.exception.AlluxioException;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.net.InetSocketAddress;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

public class AbstractClientTest {
    // test code
    // 1. You need to use the Alluxio 2.1.0 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.

    @Test
    public void testRetryRPCInternalWithClosedClient() throws Exception {
        // 1. Mock the Alluxio configuration
        AlluxioConfiguration configuration = Mockito.mock(AlluxioConfiguration.class);
        when(configuration.getBoolean(PropertyKey.CONF_VALIDATION_ENABLED)).thenReturn(true);

        // 2. Mock the AbstractClient
        AbstractClient client = Mockito.mock(AbstractClient.class);
        InetSocketAddress mockAddress = mock(InetSocketAddress.class);
        Mockito.doReturn(mockAddress).when(client).getAddress();
        Mockito.doNothing().when(client).disconnect();

        // Simulate a client being closed
        Mockito.doThrow(new StatusRuntimeException(Status.UNAVAILABLE))
                .when(client)
                .connect();

        try {
            // 3. Test the behavior of retryRPCInternal when the client is closed
            client.connect();
        } catch (StatusRuntimeException e) {
            assertEquals(Status.Code.UNAVAILABLE, e.getStatus().getCode());
        }

        // 4. Code after testing
        verify(client, times(1)).connect();
    }
}