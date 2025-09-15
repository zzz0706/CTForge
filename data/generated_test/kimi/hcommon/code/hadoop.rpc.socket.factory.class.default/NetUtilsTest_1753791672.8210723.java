package org.apache.hadoop.net;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.junit.Test;

public class NetUtilsTest {

    @Test(expected = RuntimeException.class)
    public void testClassNotAssignableToSocketFactoryThrowsException() {
        // 1. Create a Configuration instance
        Configuration conf = new Configuration();

        // 2. Set the configuration to a class that does not implement SocketFactory
        conf.set(CommonConfigurationKeysPublic.HADOOP_RPC_SOCKET_FACTORY_CLASS_DEFAULT_KEY,
                 "java.lang.Object");

        // 3. Invoke the method under test
        NetUtils.getDefaultSocketFactory(conf);

        // 4. Exception is expected; test framework will verify RuntimeException is thrown
    }
}