package com.twitter.dataservice.simulated;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import junit.framework.Assert;

import org.junit.Test;

import com.twitter.dataservice.shardutils.Vertex;

public class CompactDataNodeTest
{
    
    
    @Test
    public void testFanout(){
        CompactDataNode cdn = new CompactDataNode();
        
        int[] fanout1 = {2, 3, 4, 5, 6};
        int[] fanout2 = {0,2,4,6,8,10,12};
        int[] fanout3 = {0,3,6,9,12};
        int[] fanout4 = {0,4,8,12,16};
        
        cdn.putFanout(1, fanout1);
        cdn.putFanout(2, fanout2);
        cdn.putFanout(3, fanout3);
        cdn.putFanout(4, fanout4);

        try {
            Collection<Vertex> result1 =  getVertexList(cdn.getFanout(new Vertex(1), 10, -1));
            Assert.assertTrue(result1.equals(getVertexList(fanout1)));

            Collection<Vertex> result2 =  getVertexList(cdn.getFanout(new Vertex(2), fanout2.length, -1));
            Assert.assertTrue(result2.equals(getVertexList(fanout2)));
        } catch (RemoteException re ){
            Assert.fail();
        }
    }
    /*
     * TODO: test other methods (currently tested it somewehere else)
     */
    @Test
    public void testIntersection(){
        CompactDataNode cdn = new CompactDataNode();
        
        int[] fanout1 = {0,2,4,6,8,10,12};
        int[] fanout2 = {1,3,5,7,9,11,13};
        int[] fanout3 = {0,3,6,9,12};
        int[] fanout4 = {0,4,8,12,16};
        
        cdn.putFanout(1, fanout1);
        cdn.putFanout(2, fanout2);
        cdn.putFanout(3, fanout3);
        cdn.putFanout(4, fanout4);
        
        try {
        
        Collection<Vertex> result1 =  getVertexList(cdn.getIntersection(new Vertex(1), new Vertex(1), fanout1.length, -1));
        Assert.assertTrue(result1.equals(getVertexList(fanout1)));
        
        Collection<Vertex> result2 = getVertexList(cdn.getIntersection(new Vertex(1), new Vertex(2), fanout1.length + fanout2.length, -1));
        Assert.assertTrue(result2.equals(Collections.emptyList()));
        
        Collection<Vertex> result3 = getVertexList(cdn.getIntersection(new Vertex(2), new Vertex(3), fanout2.length + fanout3.length, -1));
        Assert.assertEquals(getVertexList(new int[]{3,9}), result3);
        
        Collection<Vertex> result4 = getVertexList(cdn.getIntersection(new Vertex(3), new Vertex(4), fanout3.length + fanout4.length, -1));
        Assert.assertEquals(getVertexList(new int[]{0,12}), result4);
        
        Collection<Vertex> offsetresult1 = getVertexList(cdn.getIntersection(new Vertex(1), new Vertex(1), 1, 0));
        Assert.assertEquals(getVertexList(new int[]{0}), offsetresult1);
        
        Collection<Vertex> offsetresult2 = getVertexList(cdn.getIntersection(new Vertex(1), new Vertex(1), 2, 0));
        Assert.assertEquals(getVertexList(new int[]{0,2}), offsetresult2);
        
        Collection<Vertex> offsetresult3 = getVertexList(cdn.getIntersection(new Vertex(1), new Vertex(1), 0, 1));
        Assert.assertEquals(Collections.EMPTY_LIST, offsetresult3);
        
        Collection<Vertex> offsetresult4 = getVertexList(cdn.getIntersection(new Vertex(1), new Vertex(1), 1, 10));
        Assert.assertEquals(getVertexList(new int[]{10}), offsetresult4);
                
        Collection<Vertex> offsetresult5 = getVertexList(cdn.getIntersection(new Vertex(1), new Vertex(2), 1, 11));
        Assert.assertEquals(Collections.EMPTY_LIST, offsetresult5);
        
        Collection<Vertex> offsetresult6 = getVertexList(cdn.getIntersection(new Vertex(3), new Vertex(4), fanout3.length + fanout4.length, 6));
        Assert.assertEquals(getVertexList(new int[]{12}), offsetresult6);
        
        Collection<Vertex> offsetresult7 = getVertexList(cdn.getIntersection(new Vertex(3), new Vertex(4), fanout3.length + fanout4.length, 13));
        Assert.assertEquals(Collections.EMPTY_LIST, offsetresult7);
     
        Collection<Vertex> offsetresult8 = getVertexList(cdn.getIntersection(new Vertex(3), new Vertex(4), fanout3.length + fanout4.length, 12));
        Assert.assertEquals(getVertexList(new int[]{12}), offsetresult8);
        
        Collection<Vertex> offsetresult9 = getVertexList(cdn.getIntersection(new Vertex(3), new Vertex(4), fanout3.length + fanout4.length, -10));
        Assert.assertEquals(getVertexList(new int[]{0,12}), offsetresult9);

        //we assume it fails if one of the vertices does not exist in the node
        try {
            getVertexList(cdn.getIntersection(new Vertex(100), new Vertex(1), 100, 100));
            Assert.fail();
        } catch (RemoteException re) {
            //TODO: make it into its own kind of exception}
        }
        
        } catch (RemoteException re){
            
        }
    }
    
    public ArrayList<Vertex> getVertexList(int[] intarray){
        ArrayList<Vertex> result = new ArrayList<Vertex>(intarray.length);
        
        for (int i: intarray){
            result.add(new Vertex(i));
        }
        
        return result;
    }
    
}
