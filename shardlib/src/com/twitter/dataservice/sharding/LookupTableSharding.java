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
import com.twitter.dataservice.shardutils.Node;
import com.twitter.dataservice.shardutils.Pair;
import com.twitter.dataservice.shardutils.Shard;
import com.twitter.dataservice.shardutils.Vertex;

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
 */
    private Map<Integer, Node> table;
    /*
     * load partition from tab separated file
     */
    public LookupTableSharding(String partitionFile, int expectedSize){
        table = new HashMap<Integer, Node>(expectedSize);

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
                String[] lines = currentLine.split("\t");
                table.put(Integer.parseInt(lines[0]), Node.getNode(Integer.parseInt(lines[1])));
            }
            
        } catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }
    
    @Override
    public Node getNode(Vertex v, Vertex w)
    {
        return table.get(v.getId());
    }

    @Override
    public Collection<Node> getNodes(Vertex v)
    {
        Collection<Node> ans = Arrays.asList(new Node[]{table.get(v.getId())});
        return ans;
    }

}
