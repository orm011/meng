package com.twitter.dataservice.shardlib;

import junit.framework.Assert;
import junit.framework.TestCase;
import org.junit.Test;

import java.util.List;
import java.util.Set;

public class DirectHashTest extends TestCase {

  @Test
  public void testVertexAndNodeSame() throws Exception {
    DirectHash dh = new DirectHash(10);
    Vertex testVer = new Vertex(0, 1);
    Vertex testVer1 = new Vertex(1, 1);
    Edge testEdge1 = new Edge(testVer, testVer);
    Edge testEdge2 = new Edge(testVer, testVer1);

    Node r1 = getUniqueNodeFromSet(dh.getReplicaSetForEdgeQuery(testEdge1));
    Node r2 = getUniqueNodeFromSet(dh.getReplicaSetForEdgeQuery(testEdge2));
    Node r3 = getUniqueNodeFromListSet(dh.getReplicaSetForVertexQuery(testVer));
    Node r4 = getUniqueNodeFromListSet(dh.getReplicaSetForVertexQuery(testVer));

    Assert.assertEquals(r1, r2);
    Assert.assertEquals(r2, r3);
    Assert.assertEquals(r3, r4);
  }

  private Node getUniqueNodeFromListSet(List<Set<Node>> lset){
    Assert.assertTrue(lset.size() == 1);

    Node answer = null;

    for (Set<Node> s: lset){
      answer = getUniqueNodeFromSet(s);
    }
    
    return answer;
  }


  private Node getUniqueNodeFromSet(Set<Node> se){
    Assert.assertTrue(se.size() == 1);

    Node answer = null;

    for (Node n: se){
      answer = n;
    }

    return answer;
  }

}
