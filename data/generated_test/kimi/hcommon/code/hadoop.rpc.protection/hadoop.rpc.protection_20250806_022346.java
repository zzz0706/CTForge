package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.junit.Test;

import java.net.InetAddress;
import java.util.Map;

import static org.junit.Assert.*;

public class SaslPropertiesResolverTest {

    @Test
    public void testValidSingleProtectionValue() {
        Configuration conf = new Configuration();
        conf.set(CommonConfigurationKeysPublic.HADOOP_RPC_PROTECTION, "privacy");

        SaslPropertiesResolver resolver = new SaslPropertiesResolver();
        resolver.setConf(conf);

        Map<String, String> props = resolver.getDefaultProperties();
        assertNotNull(props);
        assertEquals("auth-conf", props.get("javax.security.sasl.qop"));
        assertEquals("true", props.get("javax.security.sasl.server.authentication"));
    }

    @Test
    public void testValidMultipleProtectionValues() {
        Configuration conf = new Configuration();
        conf.set(CommonConfigurationKeysPublic.HADOOP_RPC_PROTECTION, "authentication,integrity,privacy");

        SaslPropertiesResolver resolver = new SaslPropertiesResolver();
        resolver.setConf(conf);

        Map<String, String> props = resolver.getDefaultProperties();
        assertNotNull(props);
        assertEquals("auth,auth-int,auth-conf", props.get("javax.security.sasl.qop"));
    }

    @Test
    public void testInvalidProtectionValueThrowsIllegalArgumentException() {
        Configuration conf = new Configuration();
        conf.set(CommonConfigurationKeysPublic.HADOOP_RPC_PROTECTION, "invalid");

        SaslPropertiesResolver resolver = new SaslPropertiesResolver();
        try {
            resolver.setConf(conf);
            fail("Expected IllegalArgumentException for invalid protection value");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("No enum constant org.apache.hadoop.security.SaslRpcServer.QualityOfProtection.INVALID"));
        }
    }

    @Test
    public void testEmptyProtectionValueUsesDefault() {
        Configuration conf = new Configuration();
        // Do not set the property at all so that the default is used
        // conf.set(CommonConfigurationKeysPublic.HADOOP_RPC_PROTECTION, "");

        SaslPropertiesResolver resolver = new SaslPropertiesResolver();
        resolver.setConf(conf);

        Map<String, String> props = resolver.getDefaultProperties();
        assertNotNull(props);
        assertEquals("auth", props.get("javax.security.sasl.qop"));
    }

    @Test
    public void testNullProtectionValueUsesDefault() {
        Configuration conf = new Configuration();

        SaslPropertiesResolver resolver = new SaslPropertiesResolver();
        resolver.setConf(conf);

        Map<String, String> props = resolver.getDefaultProperties();
        assertNotNull(props);
        assertEquals("auth", props.get("javax.security.sasl.qop"));
    }

    @Test
    public void testClientAndServerPropertiesReturnSameMap() throws Exception {
        Configuration conf = new Configuration();
        conf.set(CommonConfigurationKeysPublic.HADOOP_RPC_PROTECTION, "integrity");

        SaslPropertiesResolver resolver = new SaslPropertiesResolver();
        resolver.setConf(conf);

        InetAddress addr = InetAddress.getLocalHost();
        Map<String, String> clientProps = resolver.getClientProperties(addr);
        Map<String, String> serverProps = resolver.getServerProperties(addr);
        Map<String, String> defaultProps = resolver.getDefaultProperties();

        assertSame(clientProps, defaultProps);
        assertSame(serverProps, defaultProps);
        assertEquals("auth-int", clientProps.get("javax.security.sasl.qop"));
    }

    @Test
    public void testCaseInsensitiveProtectionValue() {
        Configuration conf = new Configuration();
        conf.set(CommonConfigurationKeysPublic.HADOOP_RPC_PROTECTION, "InTeGrItY");

        SaslPropertiesResolver resolver = new SaslPropertiesResolver();
        resolver.setConf(conf);

        Map<String, String> props = resolver.getDefaultProperties();
        assertNotNull(props);
        assertEquals("auth-int", props.get("javax.security.sasl.qop"));
    }

    @Test
    public void testWhitespaceInProtectionValue() {
        Configuration conf = new Configuration();
        conf.set(CommonConfigurationKeysPublic.HADOOP_RPC_PROTECTION, "  privacy  ,  authentication  ");

        SaslPropertiesResolver resolver = new SaslPropertiesResolver();
        resolver.setConf(conf);

        Map<String, String> props = resolver.getDefaultProperties();
        assertNotNull(props);
        assertEquals("auth-conf,auth", props.get("javax.security.sasl.qop"));
    }
}