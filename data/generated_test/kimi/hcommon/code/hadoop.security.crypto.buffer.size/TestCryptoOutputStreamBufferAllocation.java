package org.apache.hadoop.crypto;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.junit.Test;

public class TestCryptoOutputStreamBufferAllocation {

  @Test
  public void testCryptoOutputStreamBufferAllocationUsesConfiguredValue() throws Exception {
    // 1. Instantiate fresh Configuration and set a known buffer size
    Configuration conf = new Configuration(false);
    int expectedSize = 4096;
    conf.setInt(CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_BUFFER_SIZE_KEY, expectedSize);

    // 2. Prepare the test conditions: obtain a real codec instance
    CryptoCodec codec = CryptoCodec.getInstance(conf);

    // 3. Instantiate CryptoOutputStream with the real codec
    byte[] dummyKey = new byte[16];
    byte[] dummyIV = new byte[16];
    CryptoOutputStream cos = new CryptoOutputStream(
            new java.io.ByteArrayOutputStream(),
            codec,
            dummyKey,
            dummyIV,
            0L);

    // 4. Use reflection to access inBuffer and outBuffer fields
    Field inBufferField = CryptoOutputStream.class.getDeclaredField("inBuffer");
    inBufferField.setAccessible(true);
    ByteBuffer inBuffer = (ByteBuffer) inBufferField.get(cos);

    Field outBufferField = CryptoOutputStream.class.getDeclaredField("outBuffer");
    outBufferField.setAccessible(true);
    ByteBuffer outBuffer = (ByteBuffer) outBufferField.get(cos);

    // Assert capacities match expectedSize
    assertEquals("inBuffer capacity should match configured value",
            expectedSize, inBuffer.capacity());
    assertEquals("outBuffer capacity should match configured value",
            expectedSize, outBuffer.capacity());

    // 5. Code after testing: close stream
    cos.close();
  }
}