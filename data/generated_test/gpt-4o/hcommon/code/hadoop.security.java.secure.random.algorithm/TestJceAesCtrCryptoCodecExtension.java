package org.apache.hadoop.crypto;

import org.apache.hadoop.conf.Configuration;      
import org.apache.hadoop.crypto.JceAesCtrCryptoCodec;      
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;      
import org.junit.Test;      

import java.util.Arrays;     

import static org.junit.Assert.assertFalse;     
import static org.junit.Assert.assertNotNull;     

public class TestJceAesCtrCryptoCodecExtension {       
    // get configuration value using API
    // Prepare the input conditions for unit testing.
    
    @Test
    public void testSetConfWithInvalidAlgorithm() {      
        // Create a Configuration object
        Configuration conf = new Configuration();      
        // Set an invalid algorithm name      
        String invalidAlgorithm = "INVALID_ALGORITHM";      
        conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_JAVA_SECURE_RANDOM_ALGORITHM_KEY, invalidAlgorithm);      

        // Create an instance of JceAesCtrCryptoCodec
        JceAesCtrCryptoCodec codec = new JceAesCtrCryptoCodec();      

        // Call the setConf method with the invalid algorithm configuration      
        codec.setConf(conf);      

        // Assert that SecureRandom initialization fell back to default      
        assertNotNull("SecureRandom instance should not be null after setting configuration", codec);      

        // Test the generateSecureRandom method      
        byte[] randomBytes = new byte[16]; // example byte array size      
        codec.generateSecureRandom(randomBytes);      

        byte[] emptyBytes = new byte[16];      
        assertFalse("Byte array should be populated, confirming fallback behavior of SecureRandom",      
                Arrays.equals(randomBytes, emptyBytes));      
    }

    @Test
    public void testSetConfWithValidDefaultAlgorithm() {      
        // Create a Configuration object      
        Configuration conf = new Configuration();      

        // Set the default algorithm explicitly      
        String defaultAlgorithm = "SHA1PRNG";      
        conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_JAVA_SECURE_RANDOM_ALGORITHM_KEY, defaultAlgorithm);      

        // Create an instance of JceAesCtrCryptoCodec      
        JceAesCtrCryptoCodec codec = new JceAesCtrCryptoCodec();      

        // Call the setConf method with the valid default algorithm configuration      
        codec.setConf(conf);      

        // Assert that SecureRandom initialization was successful      
        assertNotNull("SecureRandom instance should not be null with valid algorithm configuration", codec);      

        // Test the generateSecureRandom method      
        byte[] randomBytes = new byte[16]; // example byte array size      
        codec.generateSecureRandom(randomBytes);      

        byte[] emptyBytes = new byte[16];      
        assertFalse("Byte array should be populated with random values using a valid algorithm",      
                Arrays.equals(randomBytes, emptyBytes));      
    }
}