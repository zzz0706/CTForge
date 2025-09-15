package alluxio.worker.grpc;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.util.network.NettyUtils;

import io.grpc.netty.NettyServerBuilder;
import io.netty.channel.EventLoopGroup;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

import static org.mockito.Mockito.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ServerConfiguration.class, NettyUtils.class})
public class GrpcDataServerNegativeBossThreadsTest {

    @Before
    public void setUp() {
        PowerMockito.mockStatic(ServerConfiguration.class);
    }

    @After
    public void tearDown() {
        ServerConfiguration.reset();
    }

    @Test(expected = RuntimeException.class)
    public void negativeBossThreadsThrowsIllegalArgument() {
        // 1. You need to use the alluxio2.1.0 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        // 2. Prepare the test conditions.
        PowerMockito.when(ServerConfiguration.getInt(PropertyKey.WORKER_NETWORK_NETTY_BOSS_THREADS))
                .thenReturn(-3);

        // 3. Test code.
        SocketAddress bindAddress = new InetSocketAddress("localhost", 29999);
        new GrpcDataServer("localhost", bindAddress, null);

        // 4. Code after testing.
    }
}