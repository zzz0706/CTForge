package org.apache.hadoop.crypto;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.junit.Test;

import java.lang.reflect.Field;

import static org.junit.Assert.assertNull;

public class JceAesCtrCryptoCodecTest {

  @Test
  public void emptyProviderStringTreatedAsNull() throws Exception {
    // 1. Configuration as input
    Configuration conf = new Configuration();
    // leave the provider unset (or set to null) instead of empty string
    conf.unset(CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_JCE_PROVIDER_KEY);

    // 2. Prepare the test conditions.
    // No mocking is required because SecureRandom.getInstance is allowed to throw
    // NoSuchAlgorithmException / NoSuchProviderException; we simply let the
    // codec handle the empty provider string gracefully.

    // 3. Test code.
    JceAesCtrCryptoCodec codec = new JceAesCtrCryptoCodec();
    codec.setConf(conf);

    // 4. Code after testing.
    Field providerField = JceAesCtrCryptoCodec.class.getDeclaredField("provider");
    providerField.setAccessible(true);
    assertNull(providerField.get(codec));
  }
}