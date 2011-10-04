package com.twitter.dataservice.sharding;

import com.twitter.dataservice.shardutils.Edge;
import com.twitter.dataservice.shardutils.Node;
import com.twitter.dataservice.shardutils.Vertex;
import junit.framework.Assert;
import junit.framework.TestCase;
import org.junit.Test;

import java.util.Collection;

public class RoundRobinShardLibTest extends TestCase {
  @Test
  public void testVertexAndNodeSame() throws Exception {
    RoundRobinShardLib dh = new RoundRobinShardLib(10);
    Vertex testVer = new Vertex(0, 1);
    Vertex testVer1 = new Vertex(1, 1);
    Edge testEdge1 = new Edge(testVer, testVer);
    Edge testEdge2 = new Edge(testVer, testVer1);

    Node r1 = dh.getNode(testEdge1);
    Node r2 = dh.getNode(testEdge2);
    Node r3 = getUniqueNodeFromCollection(dh.getNodes(testVer));
    Node r4 = getUniqueNodeFromCollection(dh.getNodes(testVer1));
    Node r5 = getUniqueNodeFromCollection(dh.getNodes(new Vertex(10,0)));
    
    Assert.assertEquals(r1, r2);
    Assert.assertEquals(r2, r3);
    Assert.assertEquals(r3, new Node(0));
    Assert.assertEquals(r4, new Node(1));
    Assert.assertEquals(r5, new Node(0));
  }

  private Node getUniqueNodeFromCollection(Collection<Node> se){
    Assert.assertTrue(se.size() == 1);
    Node answer = null;
    for (Node n: se){
      answer = n;
    }
    return answer;
  }
}