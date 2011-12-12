package com.twitter.dataservice.simulated;

import java.rmi.RemoteException;

import junit.framework.Assert;

import com.twitter.dataservice.remotes.IDataNode;
import com.twitter.dataservice.shardutils.Edge;
import com.twitter.dataservice.shardutils.Vertex;

public class CounterBackedWorkNodeTest
{
    public void testWorkNode(){
        IDataNode cbwn = new CounterBackedWorkNode();
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
            throw new RuntimeException(e);
        }
        
        
    }
    
    public void CounterBackedvsDictionaryBacked(){
        CounterBackedWorkNode counterb = new CounterBackedWorkNode();
        DictionaryBackedDataNode dictionb = new DictionaryBackedDataNode();
        
        final int[] nodes = {1, 2, 3};
        
        for (int i : nodes){
            int j = nodes[i];
            
            while (j > 0){
                Edge e = new Edge(new Vertex(i), new Vertex(--j));
                try
                {
                    counterb.putEdge(e);
                    dictionb.putEdge(e);
                } catch (RemoteException re)
                {
                    throw new RuntimeException(re);
                }
            }
        }
        
        try {
            Assert.assertEquals(counterb.totalLoad(), dictionb.totalLoad());
            Assert.assertEquals(6, counterb.totalLoad());
    
            for (int i : nodes){
                int j = nodes[i];
                Vertex left = new Vertex(i);
                
                while (j > 0){
                    Vertex right = new Vertex(j);
                    
                    Assert.assertEquals(counterb.getEdge(left, right), dictionb.getEdge(left, right));
                }
                
                Assert.assertEquals(counterb.getFanout(left), dictionb.getFanout(left));
            }
            
            try {
                dictionb.getEdge(new Vertex(0), new Vertex(1));
                Assert.fail();
            } catch (AssertionError ae){}
            
            try {
                dictionb.getFanout(new Vertex(3));
                Assert.fail();
            } catch (AssertionError ae){}
            
            dictionb.getFanout(new Vertex(0));
         
            
            //RESET
            dictionb.reset();
            try {
                dictionb.getFanout(new Vertex(0));
                Assert.fail();
            } catch (AssertionError ae){}
            
            try {
                dictionb.getEdge(new Vertex(0), new Vertex(0));
                Assert.fail();
            } catch (AssertionError ae){}
            
            Assert.assertEquals(0, dictionb.totalLoad());
            
        } catch (RemoteException re){
            throw new RuntimeException(re);
        }
        
        
   }   
}
