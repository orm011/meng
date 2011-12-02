package com.twitter.dataservice.simulated;

import java.rmi.RemoteException;

import junit.framework.Assert;

import com.twitter.dataservice.shardutils.Edge;
import com.twitter.dataservice.shardutils.Vertex;

public class CounterBackedWorkNodeTest
{
    public void testWorkNode(){
        
        CounterBackedWorkNode cbwn = null;
        try
        {
            cbwn = new CounterBackedWorkNode();
        } catch (RemoteException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        Vertex v = new Vertex(0);
        try
        {
            cbwn.putEdge(new Edge(v,v));
            cbwn.putEdge(new Edge(v,v));
            cbwn.putEdge(new Edge(v,v));

            Assert.assertEquals(0, cbwn.getFanout(new Vertex(1)).size());
            Assert.assertEquals(3, cbwn.getFanout(new Vertex(0)).size());

            cbwn.putEdge(new Edge(new Vertex(2), v));

            Assert.assertTrue(cbwn.getEdge(new Vertex(2), v) != null);
            
            Assert.assertEquals(4, cbwn.totalLoad());
            
            cbwn.reset();
            Assert.assertEquals(0, cbwn.totalLoad());
            
            try {
            cbwn.getFanout(v);
            Assert.fail();
            } catch (AssertionError e){;}
            
            
        } catch (RemoteException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
    }

}
