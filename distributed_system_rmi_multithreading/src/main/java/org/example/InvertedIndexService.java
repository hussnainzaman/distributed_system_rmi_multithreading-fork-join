package org.example;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;
import java.util.Map;

// Remote interface for Inverted Index Service
public interface InvertedIndexService extends Remote {
    
    /**
     * Retrieves the inverted index of words for a given file.
     * 
     * @param fileName The name of the file for which the inverted index is to be retrieved.
     * @return A map containing words (keys) and their positions or occurrences (values) in the file.
     * @throws RemoteException If a communication-related exception occurs during the RMI method call.
     */
    Map<String, List<Integer>> getInvertedIndex(String fileName) throws RemoteException;
}
