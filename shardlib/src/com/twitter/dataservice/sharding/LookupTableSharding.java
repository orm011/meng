package com.twitter.dataservice.sharding;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.twitter.dataservice.shardutils.Edge;
import com.twitter.dataservice.shardutils.HierarchicalHashFunction;
import com.twitter.dataservice.shardutils.Node;
import com.twitter.dataservice.shardutils.Pair;
import com.twitter.dataservice.shardutils.Shard;
import com.twitter.dataservice.shardutils.Vertex;
import com.twitter.dataservice.simulated.UtilMethods;


public class LookupTableSharding implements INodeSelectionStrategy
{
/*
 *approach 1: map id -> machine
 *  Map with O(n) pairs (id,machine)
 * 
 *   130 M ids * 4 B/id = 520MB; okay for a single jvm. *2 for the pointer to node.
 *   (can reuse nodes)
 *approach 2: keep k sets of ids, one for each machine.
 * saves us pointer to node
 * 
 * approach 3, with new data and java hashmap vertexid -> nodeid.
 * 10 million ids * 40 B/id (including linked list and pair pointers from HashMap)
 */
    private int numNodes;
    private Map<Integer, Byte> table;
    private HierarchicalHashFunction hash = new HierarchicalHashFunction();
    
    /*
     * load partition from tab separated file
     */
    public LookupTableSharding(String partitionFile, int expectedSize, int numNodes, String separator){
        table = new HashMap<Integer, Byte>(expectedSize);
        this.numNodes = numNodes;
        
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
            node = (byte) (v.hashCode() % numNodes); //default to hash
        }
        
        return UtilMethods.toNodeCollection(new byte[]{node});
    }
}