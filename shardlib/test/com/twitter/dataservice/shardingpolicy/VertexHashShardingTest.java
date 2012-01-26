package com.twitter.dataservice.shardingpolicy;

import java.util.Random;
import junit.framework.Assert;
import org.junit.Test;

public class VertexHashShardingTest
{        
    @Test
    public void testVertexHashSingleNode(){
        VertexHashSharding vhs = new VertexHashSharding(1);
        ShardpolicyTestingUtils.assertCorrectSingleNodeShard(vhs);
    }
       
    @Test
    public void testEdgeConsistentVertex(){
        for (int i = 1; i < 10; i++){
            VertexHashSharding vhs = new VertexHashSharding(i);
            ShardpolicyTestingUtils.assertEdgeMapsConsistentWithVertex(vhs);
        }
    }

    @Test
    public void testEvenDistribution(){
        for (int i = 1; i < 10; i++){
            ShardpolicyTestingUtils.assertEvenlyDistributed(new VertexHashSharding(i), 1, i);
        }
    }
    
    @Test
    public void testHash(){
        for (int i = 0; i < 100; i++){
            Assert.assertTrue(VertexHashSharding.hash((new Random()).nextInt()) >= 0);
        }
    }
    
    @Test
    public void testValidateArgs(){

        // 0 nodes should fail 
        try {
            VertexHashSharding check = new VertexHashSharding(0);
            Assert.fail();
        } catch (IllegalArgumentException e){}
        
        //less nodes than shards should fail
        try {
            VertexHashSharding check = new VertexHashSharding(1, 2);
            Assert.fail();
        } catch (IllegalArgumentException e){}
        
        // too many nodes should fail
        try {
            VertexHashSharding check = new VertexHashSharding(128, 2);
            Assert.fail();
        } catch (IllegalArgumentException e){}
        
        try {
            VertexHashSharding check = new VertexHashSharding(-1);
            Assert.fail();
        } catch (IllegalArgumentException e){}
    }
    
    @Test
    public void testVertexHashMultipleShards(){
        int MAXNUMNODES = 20;
        
        for (int numNodes = 1; numNodes < MAXNUMNODES; numNodes+=1){
            for (int numShards = 1; numShards < numNodes; numShards++){
                VertexHashSharding vhs = new VertexHashSharding(numNodes, numShards);
                ShardpolicyTestingUtils.assertEdgeMapsConsistentWithVertex(vhs);
                ShardpolicyTestingUtils.assertValidNodes(vhs, numShards, numNodes);
                ShardpolicyTestingUtils.assertEvenlyDistributed(vhs, numShards, numNodes);
                ShardpolicyTestingUtils.assertEvenlyDistributedByEdge(vhs, numShards, numNodes);
            }
        }
    }
    
    
    
}
