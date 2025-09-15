package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SaslPropertiesResolver;
import org.apache.hadoop.security.SaslRpcServer;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class SaslPropertiesResolverTest {
    @Test
    public void testSetConfValidConfigurationValues() {
        // Create a Configuration instance to fetch specified property values using API
        Configuration conf = new Configuration();
        
        // Setting up test configuration values
        conf.set("hadoop.rpc.protection", "authentication");
        
        // Instantiate SaslPropertiesResolver object
        SaslPropertiesResolver resolver = SaslPropertiesResolver.getInstance();
        
        // Pass configuration to the resolver
        resolver.setConf(conf);

        // Retrieve and verify resolved SASL Properties
        Map<String, String> saslProperties = resolver.getDefaultProperties();

        // Assertions for verifying property resolution correctness
        assertNotNull("Sasl properties should not be null.", saslProperties);
        assertTrue("Sasl properties should contain QOP key.", 
            saslProperties.containsKey(SaslRpcServer.QualityOfProtection.QOP));
        assertTrue("Sasl properties should contain SERVER AUTH key.",
            saslProperties.containsKey(SaslRpcServer.SASL_PROPS));
    }
}