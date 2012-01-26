package com.twitter.dataservice.shardingpolicy;

import org.junit.Test;

import com.google.common.primitives.Ints;


public class SimpleTwoTierShardingTest
{
    /*
     * checks the basic two tier hash behaves like a simple vertex hash when 
     * 1) there is only one shard per node (no matter the exceptions)
     * 2) there are no exceptions (no matter how many shards)
     */
    @Test
    public void testSimpleTwoTierConsistentWithVertexHash(){
        int MAXNUMNODES = 20;
        
        for (int numNodes = 1; numNodes < MAXNUMNODES; numNodes++){        
            //if numShards = 1, it should work the same no matter whats in the exception list
            SimpleTwoTierSharding stts = new SimpleTwoTierSharding(numNodes, new int[]{}, 1);
            SimpleTwoTierSharding stts2 = new SimpleTwoTierSharding(numNodes, new int[]{0}, 1);
            SimpleTwoTierSharding stts3 = new SimpleTwoTierSharding(numNodes, new int[]{0, 1}, 1);
    
            VertexHashSharding vhs = new VertexHashSharding(numNodes);
            
            int[] testkeys = new int[100];
            for(int i = 0; i < testkeys.length; i++){
                testkeys[i] = i;
            }

            ShardpolicyTestingUtils.assertEqualHashAnswers(vhs, stts, testkeys);
            ShardpolicyTestingUtils.assertEqualHashAnswers(vhs, stts2, testkeys);
            ShardpolicyTestingUtils.assertEqualHashAnswers(vhs, stts3, testkeys);        
       
            //if numShards > 1, it should work the same as long as there are no exceptions
            SimpleTwoTierSharding stts4 = new SimpleTwoTierSharding(numNodes, new int[]{}, numNodes);
            ShardpolicyTestingUtils.assertEqualHashAnswers(vhs, stts4, testkeys);
            
        }
    }
    
    /*
     * checks a composite hash behaves just like a simple one when both special and default hashes are the same
     */
    @Test
    public void testHomogeneousComposition(){
        
        int[] keys = {0, 1, 2, 3, 4};
        int[] nodes = {0, 0, 0, 0, 0};

        int[] testkeys = {5, 6, 7, 8, 9, 10};
        
        
        LookupTableSharding twin = new LookupTableSharding(keys, nodes);
        VertexHashSharding vhs = new VertexHashSharding(5, 3);
        
        //should behave the same if we give the same hash for both inputs
        ShardpolicyTestingUtils.assertEqualHashAnswers(twin, new SimpleTwoTierSharding(vhs, twin, keys), keys);
        ShardpolicyTestingUtils.assertEqualHashAnswers(vhs, new SimpleTwoTierSharding(vhs, twin, keys), testkeys);        
        ShardpolicyTestingUtils.assertEqualHashAnswers(vhs, new SimpleTwoTierSharding(vhs, vhs, keys), Ints.concat(keys, testkeys));
    }
    
    
}
