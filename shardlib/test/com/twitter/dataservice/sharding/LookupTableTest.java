package com.twitter.dataservice.sharding;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;

import junit.framework.Assert;

import org.junit.Test;

import com.twitter.dataservice.shardingpolicy.LookupTableSharding;
import com.twitter.dataservice.shardutils.Node;
import com.twitter.dataservice.shardutils.Vertex;

public class LookupTableTest
{

    /*
     * checks the lookup table file loader works
     */
    @Test
    public void testLoadingCorrect(){
        LookupTableSharding lts = new LookupTableSharding("/Users/oscarm/workspace/oscarmeng/shardlib/test/com/twitter/dataservice/sharding/testpartition.part", 1, 1, "\t");
        Assert.assertEquals(Node.getNode(1), lts.getNode(new Vertex(2), new Vertex(4)));
        Assert.assertEquals(Node.getNode(2), lts.getNode(new Vertex(4), new Vertex(1)));
        
        Collection<Node> ans = lts.getNodes(new Vertex(6));
        Assert.assertEquals(Node.getNode(3), ans.iterator().next());
        Assert.assertEquals(Node.getNode(3), lts.getNode(new Vertex(6), new Vertex(1)));
        
        Collection<Node> ans2 = lts.getNodes(new Vertex(8));
        Assert.assertEquals(Node.getNode(4), ans2.iterator().next());
        
        //for entries not in the map, we should get an exception.
        try {
            lts.getNodes(new Vertex(11));
        } catch (IllegalArgumentException e){}

    }
    
    /*
     * tests the array based constructor works as intended, and tests 
     * consistency with the file-loading one.
     */
    @Test
    public void testArrayConstructor(){
        int[] keys = {2, 4, 6, 8};
        int[] nodes = {1, 2, 3, 4};

        LookupTableSharding lts = new LookupTableSharding(keys, nodes);
        LookupTableSharding control = new LookupTableSharding("/Users/oscarm/workspace/oscarmeng/shardlib/test/com/twitter/dataservice/sharding/testpartition.part", 1, 1, "\t");

        for (int i = 0; i < keys[keys.length - 1] + 1; i++){
            Vertex curr = new Vertex(i);
            Collection<Node> actualans = new LinkedList<Node>();
            Collection<Node> controlans = new LinkedList<Node>();
            boolean actualfail = false;
            boolean controlfail = false;
            
            try {
                actualans = lts.getNodes(curr);
            } catch (IllegalArgumentException e){
                actualfail = true;
            }
            
            try {
                controlans = control.getNodes(curr);
            } catch (IllegalArgumentException e){
                controlfail = true;
            }
            
            //when one fails, both fail, when one succeeds, both succeed
            Assert.assertTrue((actualfail && controlfail) || actualans.equals(controlans));
        }
    }   
}
