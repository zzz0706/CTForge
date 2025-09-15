package org.apache.hadoop.crypto;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;

public class CryptoStreamBufferSizeTest {

    @Test
    public void testCryptoInputStreamDirectByteBufferCapacityUsesConfiguredValue() throws Exception {
        // 1. Obtain configuration value through API
        Configuration conf = new Configuration(false);
        int customSize = 4096;
        conf.setInt(CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_BUFFER_SIZE_KEY, customSize);
        int expectedSize = conf.getInt(
                CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_BUFFER_SIZE_KEY,
                CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_BUFFER_SIZE_DEFAULT);

        // 2. Prepare test conditions
        byte[] key = new byte[16];
        byte[] iv = new byte[16];

        // 3. Test code
        CryptoCodec codec = CryptoCodec.getInstance(conf, CipherSuite.AES_CTR_NOPADDING);
        CryptoInputStream cryptoInputStream = new CryptoInputStream(
                new ByteArrayInputStream(new byte[0]),
                codec,
                key,
                iv);

        // 4. Code after testing
        Field inBufferField = CryptoInputStream.class.getDeclaredField("inBuffer");
        inBufferField.setAccessible(true);
        ByteBuffer inBuffer = (ByteBuffer) inBufferField.get(cryptoInputStream);

        Field outBufferField = CryptoInputStream.class.getDeclaredField("outBuffer");
        outBufferField.setAccessible(true);
        ByteBuffer outBuffer = (ByteBuffer) outBufferField.get(cryptoInputStream);

        assertEquals(expectedSize, inBuffer.capacity());
        assertEquals(expectedSize, outBuffer.capacity());
    }

    @Test
    public void testCryptoOutputStreamDirectByteBufferCapacityUsesConfiguredValue() throws Exception {
        // 1. Obtain configuration value through API
        Configuration conf = new Configuration(false);
        int customSize = 2048;
        conf.setInt(CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_BUFFER_SIZE_KEY, customSize);
        int expectedSize = conf.getInt(
                CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_BUFFER_SIZE_KEY,
                CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_BUFFER_SIZE_DEFAULT);

        // 2. Prepare test conditions
        byte[] key = new byte[16];
        byte[] iv = new byte[16];

        // 3. Test code
        CryptoCodec codec = CryptoCodec.getInstance(conf, CipherSuite.AES_CTR_NOPADDING);
        CryptoOutputStream cryptoOutputStream = new CryptoOutputStream(
                new ByteArrayOutputStream(),
                codec,
                key,
                iv,
                0L);

        // 4. Code after testing
        Field inBufferField = CryptoOutputStream.class.getDeclaredField("inBuffer");
        inBufferField.setAccessible(true);
        ByteBuffer inBuffer = (ByteBuffer) inBufferField.get(cryptoOutputStream);

        Field outBufferField = CryptoOutputStream.class.getDeclaredField("outBuffer");
        outBufferField.setAccessible(true);
        ByteBuffer outBuffer = (ByteBuffer) outBufferField.get(cryptoOutputStream);

        assertEquals(expectedSize, inBuffer.capacity());
        assertEquals(expectedSize, outBuffer.capacity());
    }
}