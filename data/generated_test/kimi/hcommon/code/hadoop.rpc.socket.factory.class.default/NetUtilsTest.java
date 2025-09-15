package org.apache.hadoop.net;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.junit.Test;

import javax.net.SocketFactory;

import static org.junit.Assert.*;

public class NetUtilsTest {

    @Test
    public void testCustomSocketFactoryLoaded() {
        // 1. Create a Configuration instance
        Configuration conf = new Configuration();

        // 2. Prepare test conditions: ensure a dummy SocketFactory is on the classpath
        // Use a class that is already available in hadoop-common tests
        String customFactoryClassName = StandardSocketFactory.class.getName();
        conf.set(CommonConfigurationKeysPublic.HADOOP_RPC_SOCKET_FACTORY_CLASS_DEFAULT_KEY,
                 customFactoryClassName);

        // 3. Invoke the method under test
        SocketFactory actualFactory = NetUtils.getDefaultSocketFactory(conf);

        // 4. Assertions and verification
        assertNotNull("Returned SocketFactory should not be null", actualFactory);
        assertTrue("Returned SocketFactory should be an instance of StandardSocketFactory",
                   actualFactory instanceof StandardSocketFactory);
    }
}