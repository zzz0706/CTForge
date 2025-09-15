package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SaslPropertiesResolver;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.SaslDataTransferClient;
import org.junit.Test;

import java.net.InetAddress;
import java.util.Map;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

public class SaslPropertiesResolverTest {

    /**
     * Test case for setConf and getDefaultProperties validation
     * Ensures coverage of configuration propagation and resolution.
     */
    @Test
    public void testSetConfValidConfigurationValues() {
        // Get configuration value using API and prepare inputs
        Configuration conf = new Configuration();
        conf.set("hadoop.rpc.protection", "integrity");

        // Instantiate SaslPropertiesResolver object and set configuration
        SaslPropertiesResolver resolver = SaslPropertiesResolver.getInstance();
        resolver.setConf(conf);

        // Retrieve and verify resolved SASL properties
        Map<String, String> resolvedProperties = resolver.getDefaultProperties();
        
        // Validating expected outcomes for given configuration
        assertNotNull("Resolved SASL properties should not be null.", resolvedProperties);
        assertTrue("Resolved SASL properties should contain QOP key.", 
            resolvedProperties.containsKey(SaslRpcServer.QualityOfProtection.QOP));
        assertEquals("Resolved QOP value should map correctly to 'auth-int'.",
            "auth-int", resolvedProperties.get(Sasl.QOP));
        assertTrue("Resolved properties should contain SERVER_AUTH key.",
            resolvedProperties.containsKey(Sasl.SERVER_AUTH));
        assertEquals("SERVER_AUTH value should be 'true'.", 
            "true", resolvedProperties.get(Sasl.SERVER_AUTH));
    }
    
    /**
     * Test case for SaslRpcClient authentication client creation
     * Ensures the configuration propagates correctly.
     */
    @Test
    public void testSaslRpcClientCreateSaslClient() throws Exception {
        // Prepare configuration resolver
        Configuration conf = new Configuration();
        conf.set("hadoop.rpc.protection", "authentication");

        SaslPropertiesResolver resolver = SaslPropertiesResolver.getInstance();
        resolver.setConf(conf);

        SaslRpcClient saslRpcClient = new SaslRpcClient();
        InetAddress serverAddr = InetAddress.getByName("localhost");

        // Simulating a SASL Authentication type
        SaslRpcClient.SaslAuth authType = new SaslRpcClient.SaslAuth(
            "EXAMPLE_PROTOCOL", "EXAMPLE_SERVER", SaslRpcClient.AuthMethod.TOKEN.name());

        // Ensure the SASL client is created properly
        SaslClient saslClient = saslRpcClient.createSaslClient(authType);
        assertNotNull("SASL Client should be created successfully.", saslClient);
    }

    /**
     * Test case for SaslDataTransferClient stream creation
     * Verifies configuration is propagated for SASL negotiation streams.
     */
    @Test
    public void testSaslDataTransferClientGetSaslStreams() throws Exception {
        // Get configuration value using API and prepare inputs
        Configuration conf = new Configuration();
        conf.set("hadoop.rpc.protection", "privacy");

        SaslPropertiesResolver resolver = SaslPropertiesResolver.getInstance();
        resolver.setConf(conf);

        // Instantiate SaslDataTransferClient and mock inputs
        SaslDataTransferClient dataTransferClient = new SaslDataTransferClient(conf);

        InetAddress serverAddress = InetAddress.getByName("localhost");
        OutputStream mockOutput = new ByteArrayOutputStream();
        InputStream mockInput = new ByteArrayInputStream(new byte[1024]);
        Token<BlockTokenIdentifier> mockAccessToken = new Token<>("identifier".getBytes(), "password".getBytes(), new Text("kind"), new Text("service"));

        // Invoke getSaslStreams method to simulate SASL negotiation
        IOStreamPair ioStreamPair = dataTransferClient.getSaslStreams(serverAddress, mockOutput, mockInput, mockAccessToken);

        assertNotNull("IOStreamPair result should not be null.", ioStreamPair);
        assertNotNull("Output stream in IOStreamPair should not be null.", ioStreamPair.getOut());
        assertNotNull("Input stream in IOStreamPair should not be null.", ioStreamPair.getIn());
    }
}