package org.apache.hadoop.net;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import javax.net.SocketFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.api.mockito.PowerMockito;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ SocketFactory.class })
public class NetUtilsTest {

    @Test
    public void testEmptyPropertyFallsBackToJVMDefault() {
        // 1. Create Configuration instance
        Configuration conf = new Configuration();

        // 2. Set property to empty string
        conf.set(CommonConfigurationKeysPublic.HADOOP_RPC_SOCKET_FACTORY_CLASS_DEFAULT_KEY, "");

        // 3. Mock SocketFactory.getDefault() to return a known stub
        SocketFactory stubSocketFactory = new SocketFactory() {
            @Override
            public Socket createSocket() throws IOException {
                return null;
            }

            @Override
            public Socket createSocket(String host, int port) throws IOException {
                return null;
            }

            @Override
            public Socket createSocket(String host, int port, InetAddress localAddress, int localPort) throws IOException {
                return null;
            }

            @Override
            public Socket createSocket(InetAddress address, int port) throws IOException {
                return null;
            }

            @Override
            public Socket createSocket(InetAddress address, int port, InetAddress localAddress, int localPort) throws IOException {
                return null;
            }
        };

        PowerMockito.mockStatic(SocketFactory.class);
        PowerMockito.when(SocketFactory.getDefault()).thenReturn(stubSocketFactory);

        // 4. Call NetUtils.getDefaultSocketFactory(conf)
        SocketFactory actual = NetUtils.getDefaultSocketFactory(conf);

        // 5. Assert returned SocketFactory equals stub
        assertEquals(stubSocketFactory, actual);
    }
}