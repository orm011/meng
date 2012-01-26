package com.twitter.dataservice.shardingpolicy;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Random;

import junit.framework.Assert;

import com.google.common.primitives.Ints;
import com.twitter.dataservice.sharding.INodeSelectionStrategy;
import com.twitter.dataservice.shardutils.Node;
import com.twitter.dataservice.shardutils.Vertex;
import com.twitter.dataservice.simulated.UtilMethods;

public class ShardpolicyTestingUtils
{
    static Random rand = new Random();
    
    public static int randpos(){
        return Integer.MAX_VALUE & rand.nextInt();
    }
 
    public static void assertCorrectSingleNodeShard(INodeSelectionStrategy ins){
        for (int id = 0; id < 100; id++){
            Assert.assertEquals(UtilMethods.toNodeCollection(new int[]{0}), ins.getNodes(new Vertex(id)));
        }
    }
    
    public static void assertEdgeMapsConsistentWithVertex(INodeSelectionStrategy vhs){
        for (int id = 0; id < 50; id++){
            
            Assert.assertTrue(vhs.getNodes(new Vertex(id)).contains(vhs.getNode(new Vertex(id), new Vertex(randpos()))));
            Assert.assertTrue(vhs.getNodes(new Vertex(id)).contains(vhs.getNode(new Vertex(id), new Vertex(id))));
        }
    }
    
    //checks vertexids (keys) get hashed in the same ways
    public static void assertEqualHashAnswers(INodeSelectionStrategy expected, INodeSelectionStrategy given, int[] keys){
        for (int i : keys){
            Assert.assertEquals(expected.getNodes(new Vertex(i)), given.getNodes(new Vertex(i)));
        }
    }
    
    //checks vertices get evenly distributed nodes
    //checks edges also get evenly distributed among nodes they should be in
    public static void assertEvenlyDistributed(INodeSelectionStrategy vhs, int numShards, int numNodes){
        int[] tally = new int[numNodes];
        
        for (int j = 0; j < 200; j++){
            Collection<Node> ans = vhs.getNodes(new Vertex(j));
            for (Node n: ans){
                tally[n.getId()]++;
            }
        }
        
        Assert.assertTrue(Ints.min(tally) > 0);
        Assert.assertTrue((Ints.max(tally) - Ints.min(tally))/(float)Ints.min(tally) < 0.2);
    }
    
    public static void assertEvenlyDistributedByEdge(INodeSelectionStrategy vhs, int numShards, int numNodes){
        double TOLERANCE = 0.2;
        
        Vertex leftvertex = new Vertex(1);
        int[] tally = new int[numNodes];
        for (int j = 0; j < 200; j++){
                Node ans = vhs.getNode(leftvertex, new Vertex(j));
                tally[ans.getId()]++;
        }
        
        Arrays.sort(tally);
        //if there are more nodes than shards, only numShards of the entries should be 0.
        Assert.assertTrue(tally[0] >= 0);
        if (numShards < numNodes) Assert.assertEquals(0, tally[tally.length - numShards - 1]);
        Assert.assertTrue((tally[tally.length - 1] - tally[tally.length - numShards])/(double)tally[tally.length - numShards] < TOLERANCE);
    }

    //checks 1) expected number of node objects 2) valid (positive from 0 to numNodes and non repeated values)
    public static void assertValidNodes(INodeSelectionStrategy vhs, int numShards, int numNodes)
    {   
        int[] nodes = new int[numNodes];
        for (int i = 0; i < numNodes; i++){
            nodes[i] = i;
        }
        
        HashSet<Node> allNodes = new HashSet<Node>(UtilMethods.toNodeCollection(nodes));
     
        for (int j = 0; j < 100; j++){
            Collection<Node> ans = vhs.getNodes(new Vertex(j));            
            HashSet<Node> check = new HashSet<Node>(ans);
            Assert.assertEquals(numShards, check.size());
            Assert.assertTrue(allNodes.containsAll(check));        
        }
    }
}
