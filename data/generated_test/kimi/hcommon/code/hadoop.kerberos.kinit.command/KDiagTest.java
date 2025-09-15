package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import static org.junit.Assert.assertTrue;

@RunWith(PowerMockRunner.class)
@PrepareForTest({KDiag.class})
public class KDiagTest {

    @Test
    public void testKDiagWarnsWhenKinitRelativeAndNotOnPath() throws Exception {
        // 1. Create configuration without setting kinit explicitly
        Configuration conf = new Configuration();
        // Ensure the key is set to a nonexistent relative command
        conf.set("hadoop.kerberos.kinit.command", "nonexistentkinit");

        // 2. Capture diagnostic output
        ByteArrayOutputStream outContent = new ByteArrayOutputStream();
        PrintStream originalOut = System.out;
        System.setOut(new PrintStream(outContent));

        try {
            // 3. Instantiate KDiag and run validation via main() to trigger private logic
            String[] args = {};
            KDiag.main(args);

            // 4. Assert warning and PATH are printed
            String output = outContent.toString();
            assertTrue("Should warn about relative executable",
                    output.contains("Executable nonexistentkinit is relative -must be on the PATH"));
            assertTrue("Should print PATH environment variable",
                    output.contains("PATH="));
        } finally {
            System.setOut(originalOut);
        }
    }
}