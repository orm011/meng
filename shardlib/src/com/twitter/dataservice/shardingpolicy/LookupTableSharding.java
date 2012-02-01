package com.twitter.dataservice.shardingpolicy;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import com.twitter.dataservice.sharding.INodeSelectionStrategy;
import com.twitter.dataservice.shardutils.Node;
import com.twitter.dataservice.shardutils.Vertex;
import com.twitter.dataservice.simulated.UtilMethods;


public class LookupTableSharding implements INodeSelectionStrategy
{
/*
 * 10 million ids * 40 B/id (including linked list and pair pointers from HashMap)
 */
    //TODO: change to space efficient data structure
    private Map<Integer, Byte> table;
    
    /*
     * load partition from tab separated file
     */
    public LookupTableSharding(String partitionFile, int expectedSize, String separator){
        table = new HashMap<Integer, Byte>(expectedSize);
        
        BufferedReader reader;        
        try
        {
            reader = new BufferedReader(new FileReader(partitionFile)); 
        } catch (FileNotFoundException e)
        {
            throw new RuntimeException(e);
        }
        
        String currentLine;
        
        try
        {
            while ((currentLine = reader.readLine()) != null){
                String[] lines = currentLine.split(separator);
                table.put(Integer.parseInt(lines[0]), Byte.parseByte(lines[1]));
            }
            
        } catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }
    
    /*
     * keys[i] maps to value[i]
     * @param values: an integer between 0 and 127
     */
    public LookupTableSharding(int[] keys, int[] values){
        table = new HashMap<Integer, Byte>(keys.length);
        for (int i = 0; i < keys.length; i++){
            table.put(keys[i], (byte)values[i]);
        }
    }
    
    @Override
    public Node getNode(Vertex v, Vertex w)
    {   
        return getNodes(v).iterator().next();
    }

    @Override
    public Collection<Node> getNodes(Vertex v)
    {
        Byte node = table.get(v.getId());

        if (node == null){
            System.out.println("WARNING: missing lookup. This should not be being called for vertex " + v);
            throw new IllegalArgumentException("vertex not in table");
        } else {
            return UtilMethods.toNodeCollection(new byte[]{node});
        }
    }
}