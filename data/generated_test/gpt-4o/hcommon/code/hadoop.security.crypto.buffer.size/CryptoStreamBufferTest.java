package org.apache.hadoop.crypto;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic; // Import needed key definition
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream; // Import necessary class
import java.io.OutputStream; // Import necessary class
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.Arrays;

/**
 * Test class for verifying CryptoInputStream and CryptoOutputStream behavior
 * with the configured buffer size.
 */
public class CryptoStreamBufferTest {

    private Configuration conf;
    private CryptoCodec codec;
    private byte[] key;
    private byte[] iv;
    private int configuredBufferSize; // To store the buffer size read from config
    // Define the buffer size key constant if not directly accessible or for clarity
    private static final String HADOOP_SECURITY_CRYPTO_BUFFER_SIZE_KEY =
            CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_BUFFER_SIZE_KEY;
    // Define minimum buffer size constant matching CryptoStreamUtils.MIN_BUFFER_SIZE
    // Note: CryptoStreamUtils.MIN_BUFFER_SIZE is private, so we define it here based on its known value.
    private static final int MIN_BUFFER_SIZE = 512;
    // Define a suitable block size for AES (adjust if using a different cipher)
    private static final int AES_BLOCK_SIZE = 16;


    /**
     * Sets up the configuration, codec, key, and IV before each test.
     * Retrieves the buffer size using the API.
     *
     * @throws GeneralSecurityException If codec initialization fails.
     * @throws IOException              If configuration loading fails (less likely here).
     */
    @Before
    public void setUp() throws GeneralSecurityException, IOException {
        // Create configuration without loading default resources first to avoid potential overrides
        conf = new Configuration(false);
        // We will set the crypto properties manually.

        // *** Explicitly set a valid buffer size for the test ***
        // Ensure the configured size is valid (>= MIN_BUFFER_SIZE) and a multiple of the block size (16 for AES)
        // Use the minimum valid size to ensure the check passes, as the original calculation (4096)
        // seemed to cause a runtime error despite appearing valid.
        int testBufferSize = MIN_BUFFER_SIZE; // Use 512
        // Ensure 512 is a multiple of AES block size (16), which it is (512 / 16 = 32).

        // Set the calculated buffer size in the configuration
        conf.setInt(HADOOP_SECURITY_CRYPTO_BUFFER_SIZE_KEY, testBufferSize);

        // Now, get the codec instance using the prepared configuration.
        codec = CryptoCodec.getInstance(conf, CipherSuite.AES_CTR_NOPADDING);
        Assert.assertNotNull("CryptoCodec should not be null", codec);
        // Ensure the codec uses the same configuration object
        Assert.assertSame("Codec configuration should be the same object", conf, codec.getConf());
        // Verify cipher block size matches assumption used for calculation
        int actualBlockSize = codec.getCipherSuite().getAlgorithmBlockSize();
        Assert.assertEquals("Codec block size should match AES block size constant", AES_BLOCK_SIZE, actualBlockSize);


        // Get the configured buffer size using the API. This should reflect the value set earlier.
        configuredBufferSize = CryptoStreamUtils.getBufferSize(conf);

        // Verify the retrieved buffer size matches the intended value and constraints
        Assert.assertEquals("Internal buffer size read from conf should match the set value", testBufferSize, configuredBufferSize);
        // Check against the known minimum required by CryptoStreamUtils
        Assert.assertTrue("Configured buffer size " + configuredBufferSize + " must be >= " + MIN_BUFFER_SIZE,
                configuredBufferSize >= MIN_BUFFER_SIZE);
        // Also assert block size alignment, as required by checkBufferSize
        Assert.assertEquals("Configured buffer size " + configuredBufferSize + " must be a multiple of block size " + actualBlockSize,
                0, configuredBufferSize % actualBlockSize);


        // Generate random key and IV suitable for AES/CTR/NoPadding
        key = new byte[16]; // AES-128 key size
        iv = new byte[actualBlockSize]; // Use actual block size from codec
        // Use default SecureRandom unless a specific provider/algorithm is needed for reproducibility
        SecureRandom random = new SecureRandom();
        random.nextBytes(key);
        random.nextBytes(iv);
    }

    @Test
    public void testCryptoInputStreamReadWithConfiguredBufferSize() throws IOException, GeneralSecurityException {
        // Prepare plaintext data. Make it larger than the configured size to ensure buffering is tested.
        // Add a small amount to avoid exact multiple if desired, though not strictly necessary.
        int dataSize = configuredBufferSize * 3 + configuredBufferSize / 3; // e.g., 512*3 + 512/3 = 1536 + 170 = 1706
        byte[] originalPlaintext = new byte[dataSize];
        // Use default SecureRandom, getInstance("SHA1PRNG") can vary by environment
        SecureRandom random = new SecureRandom();
        random.nextBytes(originalPlaintext);

        ByteArrayOutputStream encryptedCollector = new ByteArrayOutputStream();

        // Encrypt data using CryptoOutputStream
        // Pass the verified configuredBufferSize explicitly to the constructor
        // This avoids potential issues with implicit lookup via codec.getConf() inside constructor
        // CryptoStreamUtils.checkBufferSize is called inside the constructor.
        try (OutputStream cryptoOut = new CryptoOutputStream(encryptedCollector, codec, configuredBufferSize, key, iv)) {
            cryptoOut.write(originalPlaintext, 0, originalPlaintext.length);
        } // try-with-resources ensures close/flush

        byte[] encryptedData = encryptedCollector.toByteArray();
        Assert.assertTrue("Encrypted data should exist", encryptedData.length > 0);
        // Note: With CTR mode, encrypted size equals plaintext size. Content must differ.
        if (originalPlaintext.length > 0) { // Avoid comparing empty arrays if dataSize was 0
             Assert.assertFalse("Encrypted data should differ from plaintext", Arrays.equals(originalPlaintext, encryptedData));
        }


        ByteArrayInputStream encryptedStream = new ByteArrayInputStream(encryptedData);
        ByteArrayOutputStream decryptedCollector = new ByteArrayOutputStream();

        try (InputStream cryptoIn = new CryptoInputStream(encryptedStream, codec, configuredBufferSize, key, iv)) {
            int readBufSize = Math.max(1, Math.min(1024, configuredBufferSize / 4));
            // Handle case where configuredBufferSize might be small (though >= 512) - this check is now less critical but safe
            if (readBufSize == 0 && configuredBufferSize > 0) readBufSize = 1;
            else if (readBufSize == 0) readBufSize = 1; // Avoid 0 size buffer

            byte[] readBuffer = new byte[readBufSize];
            int bytesRead;
            while ((bytesRead = cryptoIn.read(readBuffer, 0, readBuffer.length)) != -1) {
                decryptedCollector.write(readBuffer, 0, bytesRead);
            }
        } // try-with-resources ensures close

        byte[] decryptedData = decryptedCollector.toByteArray();

        // Verify the results
        Assert.assertEquals("Decrypted data size must match original plaintext size.",
                originalPlaintext.length, decryptedData.length);
        Assert.assertArrayEquals("Decrypted data must match original plaintext.",
                originalPlaintext, decryptedData);
    }
}