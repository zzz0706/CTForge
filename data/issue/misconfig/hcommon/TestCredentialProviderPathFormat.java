package org.apache.hadoop.conf;

import org.junit.Test;
import static org.junit.Assert.*;

//hadoop-14821
public class TestCredentialProviderPathFormat {

    @Test
    public void testCredentialProviderPathFormat() {
        Configuration conf = new Configuration();
        String providerPath = conf.get("hadoop.security.credential.provider.path");

        if (providerPath == null || providerPath.trim().isEmpty()) {
            return;
        }

        String pattern = "^jceks://file/\\/[^\\s]+$";
        String[] providers = providerPath.trim().split(",");
        for (String provider : providers) {
            provider = provider.trim();
            assertTrue(
                "Invalid credential provider path: " + provider,
                provider.matches(pattern)
            );
        }
    }
}
