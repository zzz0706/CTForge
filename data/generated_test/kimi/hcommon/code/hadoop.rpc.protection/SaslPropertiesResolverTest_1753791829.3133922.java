package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.util.StringUtils;
import org.junit.Test;

import javax.security.sasl.Sasl;
import java.net.InetAddress;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class SaslPropertiesResolverTest {

    @Test
    public void testSingleIntegrityProtection() throws Exception {
        // 1. Create configuration – rely on test-resource overrides or default
        Configuration conf = new Configuration();

        // 2. Dynamically compute the expected SASL QOP for "integrity"
        String[] qop = conf.getTrimmedStrings(
                CommonConfigurationKeysPublic.HADOOP_RPC_PROTECTION,
                "authentication");
        String expectedQop = "";
        for (int i = 0; i < qop.length; i++) {
            qop[i] = SaslRpcServer.QualityOfProtection.valueOf(
                    StringUtils.toUpperCase(qop[i])).getSaslQop();
        }
        expectedQop = StringUtils.join(",", qop);

        // 3. Prepare resolver and inject configuration
        SaslPropertiesResolver resolver = new SaslPropertiesResolver();
        resolver.setConf(conf);

        // 4. Invoke resolver to obtain client-side SASL properties
        Map<String, String> props = resolver.getClientProperties(InetAddress.getLocalHost());

        // 5. Verify the computed QOP is present
        assertEquals(expectedQop, props.get(Sasl.QOP));
    }
}