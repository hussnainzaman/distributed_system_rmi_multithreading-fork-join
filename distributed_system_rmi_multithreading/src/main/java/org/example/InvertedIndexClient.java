package org.example;

import java.rmi.Naming;
import java.util.List;
import java.util.Map;

public class InvertedIndexClient {
    public static void main(String[] args) {
        try {
            // Locate the registry and get the stub of the service
            String endpoint = "rmi://127.0.0.1:8099/InvertedIndexService";
            InvertedIndexService service = (InvertedIndexService) Naming.lookup(endpoint);

            // Now you can use the service object to call remote methods

            // Prepare a sample text file name
            String fileName = "sample_data.txt";

            // Invoke the service and get the inverted index
            Map<String, List<Integer>> index = service.getInvertedIndex(fileName);

            // Display the top-5 tokens with the most frequent appearance and their locations
            System.out.println("Top-5 tokens with most frequent appearance:");
            index.entrySet().stream()
                    .sorted((entry1, entry2) -> entry2.getValue().size() - entry1.getValue().size())
                    .limit(5)
                    .forEach(entry -> {
                        String token = entry.getKey();
                        List<Integer> positions = entry.getValue();
                        System.out.println("Token: " + token + ", Frequency: " + positions.size() + ", Locations: " + positions);
                    });

            // You can add more method invocations as needed

        } catch (Exception e) {
            // Print error message in case of any exception
            System.err.println("Client exception: " + e.toString());
            e.printStackTrace();
        }
    }
}
