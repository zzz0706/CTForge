package alluxio.grpc;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.AlluxioProperties;
import alluxio.conf.PropertyKey;
import alluxio.security.authentication.AuthType;
import alluxio.security.authentication.AuthenticatedUserInfo;

import io.grpc.ManagedChannel;

import java.net.InetSocketAddress;

import javax.security.auth.Subject;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({GrpcManagedChannelPool.class})
public class GrpcChannelBuilderTest {

    @Test
    public void customHealthCheckTimeoutPropagatedToChannelPool() throws Exception {
        // 1. You need to use the alluxio2.1.0 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        InstancedConfiguration conf = new InstancedConfiguration(new AlluxioProperties());
        conf.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.NOSASL.getAuthName());

        // 2. Prepare the test conditions.
        long expectedMs = conf.getMs(PropertyKey.NETWORK_CONNECTION_HEALTH_CHECK_TIMEOUT);

        GrpcManagedChannelPool mockPool = mock(GrpcManagedChannelPool.class);
        ManagedChannel mockChannel = mock(ManagedChannel.class);
        when(mockPool.acquireManagedChannel(
                any(GrpcChannelKey.class), anyLong(), anyLong()))
                .thenReturn(mockChannel);

        PowerMockito.mockStatic(GrpcManagedChannelPool.class);
        PowerMockito.when(GrpcManagedChannelPool.INSTANCE()).thenReturn(mockPool);

        // 3. Test code.
        GrpcChannelBuilder builder = GrpcChannelBuilder
                .newBuilder(GrpcServerAddress.create("localhost", new InetSocketAddress("localhost", 0)), conf)
                .setSubject(new Subject());
        builder.build(); // triggers acquireManagedChannel

        // 4. Code after testing.
        verify(mockPool).acquireManagedChannel(
                any(GrpcChannelKey.class),
                eq(expectedMs),
                anyLong());
    }
}