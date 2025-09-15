package org.apache.hadoop.security;   

import org.apache.hadoop.conf.Configuration;       
import org.apache.hadoop.security.SaslPropertiesResolver;
import org.junit.Test;
import java.net.InetAddress;
import java.util.Map;

public class TestSaslPropertiesResolver {       
    // Test code
    // 1. Use API to fetch configuration values, do not hardcode configuration values.
    // 2. Prepare test conditions
    // 3. Test code
    // 4. Post-test verification

    @Test
    public void test_GetServerProperties_IntegrityLevel() throws Exception {
        // Create a Configuration instance to fetch configuration values.
        Configuration conf = new Configuration();
        
        // Use Configuration API to set required test configuration values.
        conf.set("hadoop.security.sasl.qop", "auth-int");

        // Instantiate and configure the SaslPropertiesResolver using the configuration.
        SaslPropertiesResolver saslPropertiesResolver = SaslPropertiesResolver.getInstance(conf);

        // Simulate a client address for testing.
        InetAddress clientAddress = InetAddress.getLoopbackAddress();

        // Fetch the server properties for the client address.
        Map<String, String> serverProperties = saslPropertiesResolver.getServerProperties(clientAddress);

        // Validate the integrity QOP level (auth-int) exists in the server properties.
        assert serverProperties.containsKey("qop") : "QOP key must exist in the properties";
        assert serverProperties.get("qop").contains("auth-int") : "QOP value must resolve to 'auth-int' for integrity level";

        // Repeat for multiple client addresses to ensure consistency.
        InetAddress anotherClientAddress = InetAddress.getByName("localhost");
        Map<String, String> anotherServerProperties = saslPropertiesResolver.getServerProperties(anotherClientAddress);

        assert anotherServerProperties.containsKey("qop") : "QOP key must exist in the properties";
        assert anotherServerProperties.get("qop").contains("auth-int") : "QOP value must resolve to 'auth-int' for integrity level";
    }
}