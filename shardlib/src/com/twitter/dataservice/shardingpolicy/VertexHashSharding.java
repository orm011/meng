package com.twitter.dataservice.shardingpolicy;

import java.util.Collection;
import java.util.List;

import com.google.common.primitives.Ints;
import com.google.common.primitives.UnsignedLongs;
import com.twitter.dataservice.sharding.INodeSelectionStrategy;
import com.twitter.dataservice.sharding.ISharding;
import com.twitter.dataservice.shardutils.Edge;
import com.twitter.dataservice.shardutils.Node;
import com.twitter.dataservice.shardutils.Pair;
import com.twitter.dataservice.shardutils.Shard;
import com.twitter.dataservice.shardutils.Vertex;
import com.twitter.dataservice.simulated.UtilMethods;

public class VertexHashSharding implements INodeSelectionStrategy
{
    //the amount of data nodes in the system
    int numNodes;

    //the amount of shards per vertex. 
    int numShards;
    public VertexHashSharding(int numNodes){
        this(numNodes, 1);
    }
    
    public VertexHashSharding(int numNodes, int numShardsPerVertex){
        if (numNodes < 1 || numNodes > (int)Byte.MAX_VALUE || numShardsPerVertex > numNodes) throw new IllegalArgumentException();
        this.numNodes = numNodes;
        this.numShards = numShardsPerVertex; 
    }
    
    /*
     * returns a positive hopefully randomish integer
     */
    public static int hash(int x){
        return Integer.MAX_VALUE & Ints.hashCode(x);
    }
    
    public static int hash(int leftid, int rightid, int numNodes, int numShards){
        int start = hash(leftid) % numNodes;
        int offset = hash(rightid) % numShards;
        int nodeid = (start + offset) % numNodes;
        
        return nodeid;
    }
    
    @Override
    public Node getNode(Vertex v, Vertex w)
    {
        return new Node(hash(v.getId(), w.getId(), numNodes, numShards));
    }

    @Override
    public Collection<Node> getNodes(Vertex v)
    {        
        int basenodeid;
        int[] nodeids = new int[numShards];
        basenodeid = hash(v.getId()) % numNodes;
        
        for (int i = 0; i < numShards; i++) {
            nodeids[i] = ((basenodeid + i) % numNodes);
        }
        
        return UtilMethods.toNodeCollection(nodeids);        
    }
}