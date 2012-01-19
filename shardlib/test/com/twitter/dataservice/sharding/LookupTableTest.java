package com.twitter.dataservice.sharding;

import java.util.Collection;

import junit.framework.Assert;

import org.junit.Test;

import com.twitter.dataservice.shardutils.Node;
import com.twitter.dataservice.shardutils.Vertex;

public class LookupTableTest
{

    @Test
    public void testLoadingCorrect(){
        LookupTableSharding lts = new LookupTableSharding("/Users/oscarm/workspace/oscarmeng/shardlib/test/com/twitter/dataservice/sharding/testpartition.part", 1);
        Assert.assertEquals(Node.getNode(1), lts.getNode(new Vertex(2), new Vertex(4)));
        Assert.assertEquals(Node.getNode(2), lts.getNode(new Vertex(4), new Vertex(1)));
        
        Collection<Node> ans = lts.getNodes(new Vertex(6));
        Assert.assertEquals(Node.getNode(3), ans.iterator().next());
        Assert.assertEquals(Node.getNode(3), lts.getNode(new Vertex(6), new Vertex(1)));
        
        Collection<Node> ans2 = lts.getNodes(new Vertex(8));
        Assert.assertEquals(Node.getNode(4), ans2.iterator().next());
        
        Assert.assertNull(lts.getNodes(new Vertex(11)).iterator().next());
    }
}
