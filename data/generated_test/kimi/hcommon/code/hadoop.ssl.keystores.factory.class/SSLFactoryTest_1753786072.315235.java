package org.apache.hadoop.security.ssl;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class SSLFactoryTest {

  @Test
  public void testDestroyInvokesKeyStoresFactoryDestroy() throws Exception {
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    Configuration conf = new Configuration();
    // Provide the minimal SSL configuration required by SSLFactory in 2.8.5
    conf.set("ssl.server.keystore.location", "/tmp/keystore");
    conf.set("ssl.server.keystore.password", "password");
    conf.set("ssl.server.truststore.location", "/tmp/truststore");
    conf.set("ssl.server.truststore.password", "password");

    // 2. Prepare the test conditions.
    // Replace the factory instance with a mock via reflection
    KeyStoresFactory mockFactory = mock(KeyStoresFactory.class);
    SSLFactory sslFactory = new SSLFactory(SSLFactory.Mode.SERVER, conf);

    // Use reflection to inject the mock into the private 'keystoresFactory' field
    java.lang.reflect.Field field = SSLFactory.class.getDeclaredField("keystoresFactory");
    field.setAccessible(true);
    field.set(sslFactory, mockFactory);

    // 3. Test code.
    sslFactory.init();   // invoke init so the factory is considered initialized
    sslFactory.destroy(); // invoke destroy

    // 4. Code after testing.
    verify(mockFactory, times(1)).destroy();
  }
}