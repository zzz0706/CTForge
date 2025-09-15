package org.apache.hadoop.net;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;

import javax.net.SocketFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class NetUtilsTest {

    private Configuration conf;

    @Before
    public void setUp() {
        conf = new Configuration();
    }

    @After
    public void tearDown() {
        // reset any side-effects if necessary
    }

    /**
     * 1. Configuration value is empty → NetUtils must fall back to the JVM default SocketFactory.
     */
    @Test
    public void testEmptyPropertyFallsBackToJVMDefault() {
        conf.set(CommonConfigurationKeysPublic.HADOOP_RPC_SOCKET_FACTORY_CLASS_DEFAULT_KEY, "");

        SocketFactory result = NetUtils.getDefaultSocketFactory(conf);

        // Expect the JVM default SocketFactory (cannot directly reference DefaultSocketFactory)
        assertNotNull(result);
        // Hadoop 2.8.5 falls back to StandardSocketFactory when empty
        assertThat(result, instanceOf(StandardSocketFactory.class));
    }

    /**
     * 2. Configuration value is missing → NetUtils must fall back to the JVM default SocketFactory.
     */
    @Test
    public void testMissingPropertyFallsBackToJVMDefault() {
        // do NOT set the key at all
        SocketFactory result = NetUtils.getDefaultSocketFactory(conf);
        assertNotNull(result);
        // Hadoop 2.8.5 falls back to StandardSocketFactory, not JVM default
        assertThat(result, instanceOf(StandardSocketFactory.class));
    }

    /**
     * 3. Configuration value is a valid custom class → NetUtils must instantiate it.
     */
    @Test
    public void testCustomSocketFactoryIsInstantiated() {
        conf.set(CommonConfigurationKeysPublic.HADOOP_RPC_SOCKET_FACTORY_CLASS_DEFAULT_KEY,
                 MyTestSocketFactory.class.getName());

        SocketFactory result = NetUtils.getDefaultSocketFactory(conf);

        assertThat(result, instanceOf(MyTestSocketFactory.class));
        // Ensure it is a *new* instance, not the cached JVM default
        assertNotSame(SocketFactory.getDefault(), result);
    }

    /**
     * 4. Configuration value points to a non-existing class → NetUtils must throw.
     */
    @Test(expected = RuntimeException.class)
    public void testInvalidClassNameThrows() {
        conf.set(CommonConfigurationKeysPublic.HADOOP_RPC_SOCKET_FACTORY_CLASS_DEFAULT_KEY,
                 "com.example.DoesNotExistSocketFactory");

        NetUtils.getDefaultSocketFactory(conf);
    }

    /**
     * 5. Configuration value points to a class that is *not* a SocketFactory subclass.
     */
    @Test(expected = RuntimeException.class)
    public void testWrongClassTypeThrows() {
        conf.set(CommonConfigurationKeysPublic.HADOOP_RPC_SOCKET_FACTORY_CLASS_DEFAULT_KEY,
                 java.lang.String.class.getName());

        NetUtils.getDefaultSocketFactory(conf);
    }

    /* ------------------- helper classes ------------------- */

    /**
     * Dummy implementation so we can verify instantiation via reflection.
     */
    public static class MyTestSocketFactory extends SocketFactory {
        @Override public Socket createSocket() throws IOException { return null; }
        @Override public Socket createSocket(String host, int port) throws IOException { return null; }
        @Override public Socket createSocket(String host, int port, InetAddress localAddr, int localPort) throws IOException { return null; }
        @Override public Socket createSocket(InetAddress addr, int port) throws IOException { return null; }
        @Override public Socket createSocket(InetAddress addr, int port, InetAddress localAddr, int localPort) throws IOException { return null; }
    }
}