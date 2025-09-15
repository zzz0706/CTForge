package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import java.io.File;
import static org.mockito.Mockito.*;

public class KDiagTest {

    @Test
    public void testValidateKinitExecutable_withValidAbsolutePath() {
        // Mock Configuration instance
        Configuration mockConf = mock(Configuration.class);

        // Simulate the API returning an absolute path to a valid kinit executable
        when(mockConf.getTrimmed(KDiag.KERBEROS_KINIT_COMMAND, "")).thenReturn("/usr/bin/kinit");

        // Prepare a valid file presence scenario
        File mockFile = mock(File.class);
        when(mockFile.isAbsolute()).thenReturn(true);
        when(mockFile.exists()).thenReturn(true);
        when(mockFile.length()).thenReturn(100L); // simulate valid file existence

        // Spy on KDiag instance and inject the mock configuration and behavior
        KDiag kDiag = spy(new KDiag());
        doReturn(mockConf).when(kDiag).getConf();
        doReturn(mockFile).when(kDiag).wrapFileInstance(any(String.class));
        doNothing().when(kDiag).println(anyString(), any());

        // Run the method
        kDiag.validateKinitExecutable();

        // Verify that the method completed execution without exceptions
        verify(kDiag).println(anyString(), eq(mockFile));
        verify(mockFile).isAbsolute();
        verify(mockFile).exists();
        verify(mockFile).length();
    }
}