package com.twitter.dataservice.sharding;

import java.util.Arrays;
import java.util.Collection;

import junit.framework.Assert;

import org.junit.Test;

import com.twitter.dataservice.shardutils.Node;
import com.twitter.dataservice.shardutils.Vertex;

public class LookupTableTest
{

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
        
        //for entries not in the map, we sould get something from a hash. In this case, the only choice is 0 (1 node only)
        Assert.assertEquals(Node.getNode(0), lts.getNodes(new Vertex(11)).iterator().next());
    }
    
    @Test
    public void testLoadingLargeTable(){
        LookupTableSharding lts = new LookupTableSharding("/Users/oscarm/workspace/oscarmeng/shardlib/lookup_table_4_parts.txt", 10000000, 1, " ");
        Assert.assertEquals(Arrays.asList(new Node(2)), lts.getNodes(new Vertex(0)));
        Assert.assertEquals(Arrays.asList(new Node(1)), lts.getNodes(new Vertex(187083281)));
        Assert.assertEquals(Arrays.asList(new Node(3)), lts.getNodes(new Vertex(187083252)));
    }
    
    
}
