package org.example;

import java.io.InputStream;
import java.util.Scanner;

public class getFileContents {
    
    // Method to get the contents of a file as a String
    public String getFileContent(String fileName){
        // Create an instance of the getFileContents class
        getFileContents instance = new getFileContents();
    
        // Get the InputStream of the file
        InputStream is = instance.getFileAsIOStream(fileName);
        
        // Use try-with-resources to automatically close the Scanner
        try (Scanner s = new Scanner(is).useDelimiter("\\A")) {
            // Read the entire content of the file into a String
            String result = s.hasNext() ? s.next() : "";
            return result;
        }
    }

    // Private method to get the InputStream of a file
    private InputStream getFileAsIOStream(final String fileName) {
        // Load the file as a resource using the class loader
        InputStream ioStream = this.getClass()
            .getClassLoader()
            .getResourceAsStream(fileName);
        
        // Check if the file exists
        if (ioStream == null) {
            // Throw an exception if the file is not found
            throw new IllegalArgumentException(fileName + " is not found");
        }
        // Return the InputStream
        return ioStream;
    }
}
