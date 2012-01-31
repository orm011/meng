package com.twitter.dataservice.sharding;

import java.rmi.RemoteException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.twitter.dataservice.remotes.IDataNode;
import com.twitter.dataservice.shardingpolicy.VertexHashSharding;
import com.twitter.dataservice.shardutils.Edge;
import com.twitter.dataservice.shardutils.Node;
import com.twitter.dataservice.shardutils.Vertex;
import com.twitter.dataservice.simulated.APIServer;
import com.twitter.dataservice.simulated.CompactDataNode;
import com.twitter.dataservice.simulated.UtilMethods;

public class APIServerWithVertexHashTest
{    
    
    Map<Node, IDataNode> nodes;
    APIServer api;
    final int[] fanout0 = new int[]{1,2,3,4,5,6};
    final int[] fanout1 = new int[]{2,3,4,5,6,7,8};
    /*
     * checks the put method for the api server works, by checking
     * the effects on the data node
     */
    
    public void prepare(int numNodes){
        prepare(numNodes, numNodes);
    }
    
    public void prepare(int numNodes, int numShards){
        nodes = new HashMap<Node, IDataNode>();
        for (int i = 0; i < numNodes; i++){
            nodes.put(new Node(i), new CompactDataNode());
        }
        
        api = new APIServer(nodes, new VertexHashSharding(numNodes, numShards));

    }
    

    @After
    public void tearDown(){
        nodes = null;
        api = null;
    }
    
    @Test
    public void testPutFanoutSingleNode(){
        prepare(1);
        try
        {
            //put into the api
            api.putFanout(0, fanout0);

            //check the actual data node
            Assert.assertEquals(UtilMethods.toVertexCollection(fanout0), 
                    UtilMethods.toVertexCollection(nodes.get(Node.ZERO).getFanout(Vertex.ZERO, 10, -1)));
        } catch (RemoteException e)
        {
            Assert.fail();
        }
    }
    
    @Test 
    public void testPutFanoutTwoNodes(){
        prepare(2);     
        try
        {
            api.putFanout(0, fanout0);
            
            Collection<Vertex> expected = UtilMethods.toVertexCollection(fanout0);

            int[] actual0 = nodes.get(Node.ZERO).getFanout(Vertex.ZERO, 10, -1);
            int[] actual1 = nodes.get(Node.ONE).getFanout(Vertex.ZERO, 10, -1);
            
            //System.out.println(Arrays.toString(actual0));
            //System.out.println(Arrays.toString(actual1));
            Assert.assertTrue(actual0.length > 0 && actual1.length > 0);
            Assert.assertEquals(expected.size(), actual0.length + actual1.length);
        } catch (RemoteException e)
        {
            Assert.fail();
        }
    }
        
    
    @Test
    public void testGetFanoutOneDataNode(){
        prepare(1);
        
        api.putFanout(0, fanout0);
        
        //simple apiserver with 1 node and one shard
        Assert.assertEquals(new Edge(Vertex.ZERO, Vertex.ONE),  api.getEdge(Vertex.ZERO, Vertex.ONE));        
        Assert.assertEquals(UtilMethods.toVertexCollection(fanout0), api.getFanout(Vertex.ZERO, 10, -1));
    }
    
    @Test
    public void testGetFanoutTwoDataNodes(){
        prepare(2);
        
        api.putFanout(0, fanout0);
        
        Assert.assertEquals(new Edge(Vertex.ZERO, Vertex.ONE),  api.getEdge(Vertex.ZERO, Vertex.ONE));        
        Assert.assertEquals(UtilMethods.toVertexCollection(fanout0), api.getFanout(Vertex.ZERO, 10, -1));
    }

    /*
     * here we need to deal with the case of some fanouts being empty
     */
    @Test
    public void testGetFanoutManyDataNodes(){
        prepare(100);
        
        api.putFanout(1, fanout1);
        
        //edge
        Assert.assertEquals(new Edge(Vertex.ONE, Vertex.TWO),  api.getEdge(Vertex.ONE, Vertex.TWO));        

        //fanout
        Assert.assertEquals(UtilMethods.toVertexCollection(fanout1), api.getFanout(Vertex.ONE, 10, -1));
        
        //fanout with offset
        Assert.assertEquals(UtilMethods.toVertexCollection(new int[]{5, 6, 7, 8}), api.getFanout(Vertex.ONE, 10, 5));
        
        //page and offset
        Assert.assertEquals(UtilMethods.toVertexCollection(new int[]{5, 6, 7, 8}), api.getFanout(Vertex.ONE, 10, 5));
        
        //negative starting offset
        Assert.assertEquals(UtilMethods.toVertexCollection(new int[]{2, 3}), api.getFanout(Vertex.ONE, 2, -10));
        
        //high starting offset
        Assert.assertEquals(Collections.EMPTY_LIST, api.getFanout(Vertex.ONE, 10, 100));
        
        //TODO: add 'failed' edge query, make missing edge queries fail more graciously
    }

    
    @Test
    public void testIntersection(){
        prepare(1);
        
        api.putFanout(0, fanout0);
        api.putFanout(1, fanout1);
        
        Assert.assertEquals(UtilMethods.toVertexCollection(fanout1), api.getIntersection(Vertex.ONE, Vertex.ONE, 10, -1));
        Assert.assertEquals(UtilMethods.toVertexCollection(new int[]{2, 3, 4, 5, 6}), api.getIntersection(Vertex.ZERO, Vertex.ONE, 10, -1));
    }

    //one node gets mapped to one machine, the other the other machine
    //TODO: test both of the intersection branches independenlty, since the hash may change
    @Test
    public void testIntersectionMoreNodes(){
        prepare(2, 1);
        
        api.putFanout(0, fanout0);
        api.putFanout(1, fanout1);
        
        Assert.assertEquals(UtilMethods.toVertexCollection(fanout1), api.getIntersection(Vertex.ONE, Vertex.ONE, 10, -1));
        Assert.assertEquals(UtilMethods.toVertexCollection(new int[]{2, 3, 4, 5, 6}), api.getIntersection(Vertex.ZERO, Vertex.ONE, 10, -1));
    }    
    
    @Test
    public void testIntersectionMoreShards(){
        prepare(2, 2);
        
        api.putFanout(0, fanout0);
        api.putFanout(1, fanout1);
        
        Assert.assertEquals(UtilMethods.toVertexCollection(fanout1), api.getIntersection(Vertex.ONE, Vertex.ONE, 10, -1));
        Assert.assertEquals(UtilMethods.toVertexCollection(new int[]{2, 3, 4, 5, 6}), api.getIntersection(Vertex.ZERO, Vertex.ONE, 10, -1));
    } 
    
    
    @Test
    public void testIntersectionManyMoreShards(){
        prepare(10, 5);
        
        api.putFanout(0, fanout0);
        api.putFanout(1, fanout1);
        
        Assert.assertEquals(UtilMethods.toVertexCollection(fanout1), api.getIntersection(Vertex.ONE, Vertex.ONE, 10, -1));
        Assert.assertEquals(UtilMethods.toVertexCollection(new int[]{2, 3, 4, 5, 6}), api.getIntersection(Vertex.ZERO, Vertex.ONE, 10, -1));
    } 

}
