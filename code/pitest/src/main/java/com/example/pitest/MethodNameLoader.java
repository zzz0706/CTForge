package com.example.pitest;

import java.io.File;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Set;

public class MethodNameLoader {
    
  
    private static final String methodNamesFile = "resources/method_names.txt";
    

    public static Set<String> loadMethodNamesFromFile() {
        Set<String> names = new LinkedHashSet<>();
        File file = new File(methodNamesFile);
        if (!file.exists()) {
         
            return names;
        }
        
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (!line.isEmpty()) {
                    names.add(line);
                }
            }
        } catch (IOException e) {
            System.err.println( e.getMessage());
        }
        return names;
    }
    
    public static void main(String[] args) {
        Set<String> names = loadMethodNamesFromFile();
        for (String name : names) {
            System.out.println(name);
        }
    }
} 