package com.twitter.dataservice.shardutils;

import junit.framework.Assert;

public class UtilClassesTest
{
    
    public void testVertex(){
        Vertex v1 = new Vertex(1);
        Vertex v2 = new Vertex(2);
        Vertex v3 = new Vertex(1);
        
        Assert.assertEquals(v1, v3);
        Assert.assertEquals(v1.hashCode(), v3.hashCode());
        
        Assert.assertFalse(v1.equals(v2));
    }
    
    public void testEdge(){
        Edge e = new Edge(new Vertex(1), new Vertex(2));
        Edge e2 = new Edge(new Vertex(1), new Vertex(2));
        Edge e3 = new Edge(new Vertex(2), new Vertex(1));
        
        Assert.assertEquals(e, e2);
        Assert.assertEquals(e.hashCode(), e2.hashCode());
        
        Assert.assertFalse(e2.equals(e3));
        
        Assert.assertEquals(e.getLeftEndpoint(), e2.getLeftEndpoint());
        Assert.assertEquals(e2.getLeftEndpoint(), e3.getRightEndpoint());
    }
    
    public void testPair(){
        Vertex v1 = new Vertex(1);
        Vertex v2 = new Vertex(2);
        
        Vertex u1 = new Vertex(1);
        Vertex u2 = new Vertex(2);
        
        Pair<Vertex, Vertex> p1 = new Pair<Vertex, Vertex>(v1, v2);
        Pair<Vertex, Vertex> p2 = new Pair<Vertex, Vertex>(u1, u2);
        Pair<Vertex, Vertex> p3 = new Pair<Vertex, Vertex>(u2, u1);
        
        Assert.assertEquals(p1, p2);
        Assert.assertFalse(p1.equals(p3));
        
        Assert.assertEquals(p1.hashCode(), p2.hashCode());
        
        //not necessary but my intensions are they are different
        Assert.assertFalse(p1.hashCode() == p3.hashCode());
    }

}
