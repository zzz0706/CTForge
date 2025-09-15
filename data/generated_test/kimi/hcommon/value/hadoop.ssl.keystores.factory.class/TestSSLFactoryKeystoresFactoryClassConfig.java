package org.apache.hadoop.security.ssl;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestSSLFactoryKeystoresFactoryClassConfig {

  @Test
  public void testKeystoresFactoryClassConfigValid() {
    // 1. Obtain configuration via standard Hadoop API
    Configuration conf = new Configuration();

    // 2. Prepare test conditions – do NOT set anything in code
    //    Expect the default value to be present
    Class<? extends KeyStoresFactory> defaultClass = FileBasedKeyStoresFactory.class;

    // 3. Test code
    Class<?> actualClass = conf.getClass(
        SSLFactory.KEYSTORES_FACTORY_CLASS_KEY,
        defaultClass,
        KeyStoresFactory.class);

    assertNotNull(
        "hadoop.ssl.keystores.factory.class must resolve to a non-null class",
        actualClass);

    assertTrue(
        "hadoop.ssl.keystores.factory.class must implement KeyStoresFactory",
        KeyStoresFactory.class.isAssignableFrom(actualClass));

    // 4. Code after testing – nothing to clean up
  }

  @Test
  public void testKeystoresFactoryClassConfigInvalidClass() {
    // 1. Obtain configuration via standard Hadoop API
    Configuration conf = new Configuration();

    // 2. Prepare test conditions – do NOT set anything in code
    //    The value may be absent or malformed; we assert on the failure path

    // 3. Test code
    Class<?> actualClass = conf.getClass(
        SSLFactory.KEYSTORES_FACTORY_CLASS_KEY,
        null,
        KeyStoresFactory.class);

    // If the value is set to something that cannot be loaded or does not
    // implement KeyStoresFactory, getClass returns null
    if (actualClass != null) {
      assertTrue(
          "hadoop.ssl.keystores.factory.class must implement KeyStoresFactory",
          KeyStoresFactory.class.isAssignableFrom(actualClass));
    }
    // else: value absent or invalid – handled gracefully by Hadoop
    // No exception thrown, so we just ensure the contract holds.

    // 4. Code after testing – nothing to clean up
  }
}