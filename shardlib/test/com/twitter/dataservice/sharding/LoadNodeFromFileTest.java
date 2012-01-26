package com.twitter.dataservice.sharding;

import java.rmi.RemoteException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import junit.framework.Assert;

import org.junit.Test;

import com.twitter.dataservice.remotes.IDataNode;
import com.twitter.dataservice.shardutils.Edge;
import com.twitter.dataservice.shardutils.Vertex;
import com.twitter.dataservice.simulated.CompactDataNode;
import com.twitter.dataservice.simulated.DictionaryBackedDataNode;
import com.twitter.dataservice.simulated.UtilMethods;
import com.twitter.dataservice.simulated.WorkNodeMain;

public class LoadNodeFromFileTest
{
    @Test
    public void testLoad(){
        CompactDataNode dn = new CompactDataNode(2);
        WorkNodeMain.loadFromLocal("/Users/oscarm/workspace/oscarmeng/shardlib/test/com/twitter/dataservice/sharding/edgeFile.edges", dn);

        try {
            Edge e1 = dn.getEdge(new Vertex(1), new Vertex(2));
            Edge e2 = dn.getEdge(new Vertex(2), new Vertex(3));
            Edge e3 = dn.getEdge(new Vertex(1), new Vertex(3));
            
            Collection<Vertex> fan = dn.getFanout(new Vertex(1));
            Collection<Vertex> fan2 = dn.getFanout(new Vertex(2));

            Assert.assertEquals(new Edge(new Vertex(1), new Vertex(2)), e1);
            Assert.assertEquals(new Edge(new Vertex(2), new Vertex(3)), e2);
            Assert.assertEquals(new Edge(new Vertex(1), new Vertex(3)), e3);
            
            Assert.assertEquals(2, fan.size());
            Assert.assertEquals(1, fan2.size());
            
            Set<Vertex> setversion = new HashSet<Vertex>(fan);
            
            Assert.assertTrue(setversion.contains(new Vertex(2)));
            Assert.assertTrue(setversion.contains(new Vertex(3)));
            
            try {
                dn.getEdge(new Vertex(3), new Vertex(1));
                Assert.fail();
            } catch (AssertionError ae){
            }
            
            
        } catch (RemoteException re){
            Assert.fail();
        }
    }
}
