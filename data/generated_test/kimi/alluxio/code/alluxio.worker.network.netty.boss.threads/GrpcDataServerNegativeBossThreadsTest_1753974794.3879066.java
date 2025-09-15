package alluxio.worker.grpc;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.util.network.NettyUtils;

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
        // 1. Prepare the test conditions: mock ServerConfiguration to return -3
        PowerMockito.when(ServerConfiguration.getInt(PropertyKey.WORKER_NETWORK_NETTY_BOSS_THREADS))
                .thenReturn(-3);

        // 2. Test code: attempt to instantiate GrpcDataServer
        SocketAddress bindAddress = new InetSocketAddress("localhost", 29999);
        new GrpcDataServer("localhost", bindAddress, null);
    }
}