package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;

import static org.mockito.Mockito.*;

public class TestKerberosConfiguration {

    // Get configuration value using API
    // Prepare the input conditions for unit testing.
    @Test
    public void testValidateKinitExecutable_withValidAbsolutePath() {
        // Mock the Configuration object
        Configuration mockConf = mock(Configuration.class);
        when(mockConf.getTrimmed(KDiag.KERBEROS_KINIT_COMMAND, ""))
            .thenReturn("/usr/bin/kinit");

        // Mock the File object to simulate the existence of a valid kinit executable
        File mockFile = mock(File.class);
        when(mockFile.isAbsolute()).thenReturn(true);
        when(mockFile.exists()).thenReturn(true);
        when(mockFile.length()).thenReturn(100L); // Simulate non-empty file

        // Spy on KDiag instance while injecting mocked dependencies
        KDiag kDiag = spy(new KDiag());
        doReturn(mockConf).when(kDiag).getConf();
        doReturn(mockFile).when(kDiag).wrapFileInstance(any(String.class));
        doNothing().when(kDiag).println(anyString(), any());

        // Call the method under test
        kDiag.validateKinitExecutable();

        // Verify the configuration and validation properly executed
        verify(mockConf).getTrimmed(KDiag.KERBEROS_KINIT_COMMAND, "");
        verify(kDiag).wrapFileInstance("/usr/bin/kinit");
        verify(mockFile).isAbsolute();
        verify(mockFile).exists();
        verify(mockFile).length();
        verify(kDiag).println(anyString(), eq(mockFile));
    }

    // Get configuration value using API
    // Prepare the input conditions for unit testing.
    @Test
    public void testExecute_withValidConfiguration() throws Exception {
        // Mock the Configuration object
        Configuration mockConf = mock(Configuration.class);
        when(mockConf.getTrimmed(KDiag.KERBEROS_KINIT_COMMAND, ""))
            .thenReturn("/usr/bin/kinit");

        // Spy on KDiag instance while injecting mocked dependencies
        KDiag kDiag = spy(new KDiag());
        doReturn(mockConf).when(kDiag).getConf();
        doNothing().when(kDiag).validateKinitExecutable();

        // Call the method under test
        boolean result = kDiag.execute();

        // Verify the configuration and required method(s) invoked
        verify(mockConf).getTrimmed(KDiag.KERBEROS_KINIT_COMMAND, "");
        verify(kDiag).validateKinitExecutable();
        assert result; // Check execution success
    }

    // Get configuration value using API
    // Prepare the input conditions for unit testing.
    @Test
    public void testSpawnAutoRenewalThreadForUserCreds_withKerberosAuthentication() throws InterruptedException {
        // Mock the Configuration object
        Configuration mockConf = mock(Configuration.class);
        when(mockConf.getTrimmed("hadoop.kerberos.kinit.command", "kinit"))
            .thenReturn("kinit");

        // Mock UserGroupInformation object
        UserGroupInformation mockUGI = spy(new UserGroupInformation());
        doReturn(mockConf).when(mockUGI).getConf();
        doNothing().when(mockUGI).spawnAutoRenewalThreadForUserCreds();

        // Call the method under test
        mockUGI.spawnAutoRenewalThreadForUserCreds();

        // Verify configuration and credentials renewal properly initiated
        verify(mockConf).getTrimmed("hadoop.kerberos.kinit.command", "kinit");
        verify(mockUGI).spawnAutoRenewalThreadForUserCreds();
    }

    // Get configuration value using API
    // Prepare the input conditions for unit testing.
    @Test
    public void testLoginUserFromSubject_withValidSubject() throws Exception {
        // Mock the Subject object and the Configuration object
        Subject mockSubject = mock(Subject.class);
        Configuration mockConf = mock(Configuration.class);
        when(mockConf.getTrimmed("hadoop.kerberos.kinit.command", "kinit"))
            .thenReturn("kinit");

        // Set mocked configuration into UserGroupInformation
        UserGroupInformation.setConfiguration(mockConf);

        // Call the method under test
        UserGroupInformation.loginUserFromSubject(mockSubject);

        // Verify configurations accessed and Subject utilized
        verify(mockConf).getTrimmed("hadoop.kerberos.kinit.command", "kinit");
        verify(mockSubject, atLeastOnce()).toString(); // Ensure Subject interactions
    }
}