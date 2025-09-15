package org.apache.hadoop.crypto;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;

public class CryptoInputStreamBufferSizeTest {

    @Test
    public void testCryptoInputStreamDirectByteBufferCapacityUsesConfiguredValue() throws Exception {
        // 1. Configuration as input
        Configuration conf = new Configuration();
        // ensure the AES codec is available
        conf.set("hadoop.security.crypto.codec.classes.aes.ctr.nopadding",
                 "org.apache.hadoop.crypto.OpensslAesCtrCryptoCodec,org.apache.hadoop.crypto.JceAesCtrCryptoCodec");

        // 2. Dynamic expected value calculation
        int expectedSize = conf.getInt(
                CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_BUFFER_SIZE_KEY,
                CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_BUFFER_SIZE_DEFAULT);

        // 3. Prepare the test conditions
        byte[] key = new byte[16];
        byte[] iv = new byte[16];

        // 4. Test code
        CryptoCodec codec = CryptoCodec.getInstance(conf, CipherSuite.AES_CTR_NOPADDING);
        CryptoInputStream cryptoInputStream = new CryptoInputStream(
                new ByteArrayInputStream(new byte[0]),
                codec,
                key,
                iv);

        // 5. Code after testing
        Field inBufferField = CryptoInputStream.class.getDeclaredField("inBuffer");
        inBufferField.setAccessible(true);
        ByteBuffer inBuffer = (ByteBuffer) inBufferField.get(cryptoInputStream);

        Field outBufferField = CryptoInputStream.class.getDeclaredField("outBuffer");
        outBufferField.setAccessible(true);
        ByteBuffer outBuffer = (ByteBuffer) outBufferField.get(cryptoInputStream);

        assertEquals(expectedSize, inBuffer.capacity());
        assertEquals(expectedSize, outBuffer.capacity());
    }
}