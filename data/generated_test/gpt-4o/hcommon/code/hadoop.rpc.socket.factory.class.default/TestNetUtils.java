package org.apache.hadoop.net;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import javax.net.SocketFactory;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class TestNetUtils {
    //test code
    @Test
    public void testGetDefaultSocketFactory_withNullOrEmptyConfigKey() {
    
        // Create a Hadoop Configuration object
        Configuration conf = new Configuration();

        String defaultSocketFactoryKey = "hadoop.rpc.socket.factory.class.default";


        // Verify behavior for key set to a valid value (use default JVM socket factory)
        conf.set(defaultSocketFactoryKey, SocketFactory.getDefault().getClass().getName());
        SocketFactory socketFactoryForValidKey = NetUtils.getDefaultSocketFactory(conf);
        assertEquals("Expected JVM default socket factory when the config key is set to a valid value.",
                SocketFactory.getDefault().getClass().getName(),
                socketFactoryForValidKey.getClass().getName());

        // Verify behavior for key set to an empty string (this defaults to DefaultSocketFactory in Hadoop)
        conf.set(defaultSocketFactoryKey, "");
        SocketFactory socketFactoryForEmptyKey = NetUtils.getDefaultSocketFactory(conf);
        assertEquals("Expected DefaultSocketFactory when the config key is set to an empty string.",
                javax.net.SocketFactory.getDefault().getClass().getName(),
                socketFactoryForEmptyKey.getClass().getName());

        // Verify behavior for key unset (this defaults to StandardSocketFactory in Hadoop)
        conf.unset(defaultSocketFactoryKey);
        SocketFactory socketFactoryForUnsetKey = NetUtils.getDefaultSocketFactory(conf);
        assertEquals("Expected StandardSocketFactory when the config key is unset.",
                org.apache.hadoop.net.StandardSocketFactory.class.getName(),
                socketFactoryForUnsetKey.getClass().getName());
    }
}