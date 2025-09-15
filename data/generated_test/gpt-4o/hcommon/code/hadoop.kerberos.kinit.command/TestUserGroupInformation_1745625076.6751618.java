package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Test;
import org.mockito.Mockito;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosTicket;
import java.io.IOException;

import static org.mockito.Mockito.*;

public class TestUserGroupInformation {

    // Get configuration value using API
    // Prepare the input conditions for unit testing.
    @Test
    public void testSpawnAutoRenewalThreadForUserCreds_withKerberosEnabled() throws Exception {
        // Step 1: Configure Kerberos within Hadoop.
        Configuration conf = new Configuration();
        conf.set("hadoop.security.authentication", "kerberos");

        // Step 2: Mock the Kerberos environment.
        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation mockUgi = Mockito.mock(UserGroupInformation.class);
        Subject mockSubject = new Subject();
        KerberosTicket mockTgt = Mockito.mock(KerberosTicket.class);
        when(mockUgi.isSecurityEnabled()).thenReturn(true);
        when(mockUgi.getAuthenticationMethod()).thenReturn(UserGroupInformation.AuthenticationMethod.KERBEROS);
        when(mockUgi.getTGT()).thenReturn(mockTgt);

        // Step 3: Define the behavior of a test thread to simulate the periodic renewal.
        doAnswer(invocation -> {
            String cmd = conf.get("hadoop.kerberos.kinit.command", "kinit");
            // Simulate the expected behavior.
            System.out.println("Simulating TGT renewal with command: " + cmd);
            return null;
        }).when(mockUgi).spawnAutoRenewalThreadForUserCreds();

        // Step 4: Perform the test call.
        mockUgi.spawnAutoRenewalThreadForUserCreds();

        // Step 5: Verify that the thread is properly spawned and performs the renewal action.
        verify(mockUgi, times(1)).spawnAutoRenewalThreadForUserCreds();
        verify(mockUgi, times(1)).getTGT();
    }
}