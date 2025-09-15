package org.apache.hadoop.conf;

import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.net.NetUtils;
import org.junit.Test;

import javax.net.SocketFactory;
import static org.junit.Assert.*;

public class TestHadoopRpcSocketFactoryClassDefaultConfig {

  @Test
  public void testValidSocketFactoryClass() {
    Configuration conf = new Configuration(false);
    // Do NOT set the value in code – read whatever the user provided
    String propValue = conf.get(
        CommonConfigurationKeysPublic.HADOOP_RPC_SOCKET_FACTORY_CLASS_DEFAULT_KEY,
        CommonConfigurationKeysPublic.HADOOP_RPC_SOCKET_FACTORY_CLASS_DEFAULT_DEFAULT);

    // If the value is empty or null, NetUtils falls back to JVM default – valid
    if (propValue == null || propValue.trim().isEmpty()) {
      assertNotNull(NetUtils.getDefaultSocketFactory(conf));
      return;
    }

    // Otherwise the value must name a loadable class that implements SocketFactory
    try {
      Class<?> clazz = conf.getClassByName(propValue);
      assertTrue("Configured SocketFactory must implement javax.net.SocketFactory",
                 SocketFactory.class.isAssignableFrom(clazz));
    } catch (ClassNotFoundException e) {
      fail("Configured SocketFactory class not found: " + propValue);
    }
  }
}