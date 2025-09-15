package org.apache.zookeeper.server.quorum;

import org.junit.Test;
import static org.junit.Assert.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

//ZOOKEEPER-1878
public class QuorumPeerConfigConfigFileTest {
    private static final String CONFIG_PATH =
 
    @Test
 * 
    public void testConfigFileDataLogDirRobustness() throws Exception {
        // 1. Load properties from config file
        Properties props = new Properties();
        try (InputStream in = new FileInputStream(CONFIG_PATH)) {
            props.load(in);
        }

        // 2. Check if dataLogDir is set, and if so, delete it to ensure it does not exist
        String dataLogDirPath = props.getProperty("dataLogDir");
        if (dataLogDirPath != null) {
            File dataLogDir = new File(dataLogDirPath.trim());
            if (dataLogDir.exists()) {
                deleteRecursively(dataLogDir); 
            }
        }

        // 3. Parse properties, should not crash even if dataLogDir is missing
        QuorumPeerConfig config = new QuorumPeerConfig();
        boolean parseFailed = false;
        Exception thrown = null;
        try {
            config.parseProperties(props);
        } catch (Exception e) {
            parseFailed = true;
            thrown = e;
        }

        if (parseFailed) {
            System.err.println("Config parsing failed with exception: " + thrown);
        }
        assertFalse("Config parsing should not fail when dataLogDir is missing", parseFailed);
    }

    // Recursively delete file/directory
    private static void deleteRecursively(File file) {
        if (file.isDirectory()) {
            File[] files = file.listFiles();
            if (files != null) for (File f : files) deleteRecursively(f);
        }
        file.delete();
    }
}

                
                    