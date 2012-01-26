package com.twitter.dataservice.shardingpolicy;

import java.util.Collection;
import java.util.HashSet;

import com.twitter.dataservice.sharding.INodeSelectionStrategy;
import com.twitter.dataservice.shardutils.Node;
import com.twitter.dataservice.shardutils.Vertex;
import com.twitter.dataservice.simulated.UtilMethods;


/*
 * unlike the other TwoTierHashSharding class, this one uses no hashring (since for what I am doing, I have no concept of 'shard',
 * nor need to define one using ranges)
 * only of partitioing data. This sharding policy can be easily computed by a hadoop job that prepartitions the data before I load it.
 */
public class SimpleTwoTierSharding implements INodeSelectionStrategy
{
    INodeSelectionStrategy defaultsharding;
    INodeSelectionStrategy specialsharding;
    HashSet<Integer> exceptionset; 
    
    //TODO: evaluate if making loading exception concurrent to speed it up (depending on #exceptions eg 9million)
    //or if need to make more compact
    public SimpleTwoTierSharding(int numNodes, int[] specialids, int numShardsForExceptions){
        this(new VertexHashSharding(numNodes), new VertexHashSharding(numNodes, numShardsForExceptions), specialids);        
    }        
    
    public SimpleTwoTierSharding(INodeSelectionStrategy defaultsharding, INodeSelectionStrategy specialsharding, int[] specialids){
        this.defaultsharding = defaultsharding;
        this.specialsharding = specialsharding;
        
        exceptionset = new HashSet<Integer>(specialids.length);
        for (int specialid: specialids){
            exceptionset.add(specialid);
        }
    }
    
    @Override
    public Node getNode(Vertex v, Vertex w)
    {        
        if (exceptionset.contains(v.getId())){
            return specialsharding.getNode(v, w);
        } else {
            return defaultsharding.getNode(v, w);
        }
    }

    @Override
    public Collection<Node> getNodes(Vertex v)
    {
        if (exceptionset.contains(v.getId())){
            return specialsharding.getNodes(v);
        } else {
            return defaultsharding.getNodes(v);
        }
    }
}