package com.example.pitest;

import java.util.Set;

public class Main {
    public static void main(String[] args) {
        Set<String> names = MethodNameLoader.loadMethodNamesFromFile();
        for (String name : names) {
            System.out.println(name);
        }
    }
}