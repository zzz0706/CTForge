package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.junit.Test;

import static org.junit.Assert.*;

import java.io.IOException;

// HADOOP-14155
public class TestHadoopSecurityAuthToLocal {

    @Test
    public void testAuthToLocalConfiguration() {
        Configuration conf = new Configuration();

        String authToLocalRules = conf.get("hadoop.security.auth_to_local");

        if (authToLocalRules == null) {
            KerberosName.setRules("DEFAULT");
            assertNotNull("KerberosName rules should be initialized when config is null",
                KerberosName.getRules());
        } else {
            try {
                KerberosName.setRules(authToLocalRules);
                assertNotNull("KerberosName rules should be initialized from config",
                    KerberosName.getRules());
            } catch (IllegalArgumentException e) {
                fail("Invalid hadoop.security.auth_to_local rules: " + e.getMessage());
            }
        }
    }


    @Test
    public void testKerberosNameTranslation() throws IOException {
        Configuration conf = new Configuration();

        String authToLocalRules = conf.get("hadoop.security.auth_to_local");
        KerberosName.setRules(authToLocalRules != null ? authToLocalRules : "DEFAULT");

        KerberosName kerberosName = new KerberosName("user@EXAMPLE.COM");
        String shortName = kerberosName.getShortName(); 

        assertNotNull("Translated short name should not be null", shortName);

        System.out.println("Kerberos shortName = [" + shortName + "]");
    }
}
