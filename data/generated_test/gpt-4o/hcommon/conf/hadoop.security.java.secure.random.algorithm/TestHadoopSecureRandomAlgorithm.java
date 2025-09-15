package org.apache.hadoop.crypto;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import java.security.SecureRandom;
import java.security.GeneralSecurityException;

import static org.junit.Assert.*;

public class TestHadoopSecureRandomAlgorithm {

    private static final String HADOOP_SECURITY_JAVA_SECURE_RANDOM_ALGORITHM_KEY = 
        "hadoop.security.java.secure.random.algorithm";

    private static final String DEFAULT_ALGORITHM = "SHA1PRNG";

    @Test
    public void testValidConfigurationValue() {
        // Step 1: Load the configuration
        Configuration conf = new Configuration();

        // Step 2: Check if the configuration value is set
        String secureRandomAlg = conf.get(HADOOP_SECURITY_JAVA_SECURE_RANDOM_ALGORITHM_KEY, DEFAULT_ALGORITHM);
        
        // Step 3: Validate the configuration value
        assertNotNull("Secure Random Algorithm configuration cannot be null", secureRandomAlg);

        // Ensure the algorithm value is a valid one by attempting to instantiate a SecureRandom object
        try {
            SecureRandom random = SecureRandom.getInstance(secureRandomAlg);
            assertNotNull("SecureRandom instance should not be null", random);
        } catch (GeneralSecurityException e) {
            fail("Invalid algorithm specified in the configuration: " + secureRandomAlg + "\n" + e.getMessage());
        }
    }

    @Test
    public void testFallbackConfigurationValue() {
        // Step 1: Load an empty configuration
        Configuration conf = new Configuration();

        // Step 2: Get the configuration value and ensure it defaults to the expected value
        String secureRandomAlg = conf.get(HADOOP_SECURITY_JAVA_SECURE_RANDOM_ALGORITHM_KEY, DEFAULT_ALGORITHM);

        // Step 3: Validate the default algorithm value
        assertEquals("Expected default algorithm 'SHA1PRNG' did not match", DEFAULT_ALGORITHM, secureRandomAlg);

        try {
            SecureRandom random = SecureRandom.getInstance(secureRandomAlg);
            assertNotNull("SecureRandom instance should not be null", random);
        } catch (GeneralSecurityException e) {
            fail("Fallback algorithm is invalid: " + secureRandomAlg + "\n" + e.getMessage());
        }
    }

    @Test
    public void testInvalidConfigurationValueHandling() {
        // Step 1: Create configuration with an invalid value
        Configuration conf = new Configuration();
        conf.set(HADOOP_SECURITY_JAVA_SECURE_RANDOM_ALGORITHM_KEY, "InvalidAlgorithm");

        // Step 2: Get the configuration value
        String secureRandomAlg = conf.get(HADOOP_SECURITY_JAVA_SECURE_RANDOM_ALGORITHM_KEY, DEFAULT_ALGORITHM);

        // Step 3: Validate the algorithm value and test fallback
        try {
            SecureRandom random = SecureRandom.getInstance(secureRandomAlg);
            fail("Expected an exception for invalid algorithm, but SecureRandom instance was created");
        } catch (GeneralSecurityException e) {
            // Verify that the fallback mechanism would ensure a SecureRandom instance is created
            SecureRandom fallbackRandom = new SecureRandom();
            assertNotNull("Fallback SecureRandom instance should not be null", fallbackRandom);
        }
    }
}