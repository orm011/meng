package com.twitter.dataservice.simulated;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import com.google.common.primitives.Ints;
import com.twitter.dataservice.remotes.IDataNode;
import com.twitter.dataservice.shardutils.Edge;
import com.twitter.dataservice.shardutils.Vertex;


/*
 * TODO: how do we make sure there are enough buckets in the hashtable?
 */
public class CompactDataNode extends AbstractDataNode implements IDataNode
{
    Logger log = LoggerFactory.getLogger(CompactDataNode.class);
    //consistency condition: there is an edge (u, v) for every v in fanout(u)
    //note the edge object also contains the pair.    
    private HashMap<Integer, int[]> fanouts;
    private final String name;   
    
    public CompactDataNode(int numVertices){
        String logPropertyFile = "dataNodelog4j.properties";
        PropertyConfigurator.configure(logPropertyFile);
        
        fanouts = new HashMap<Integer, int[]>(numVertices);
        name = "defaultName";
    }
    
    public CompactDataNode(){
        fanouts = new HashMap<Integer, int[]>();
        name = "defaultName";
    }
    
    @Override
    public Edge getEdge(Vertex left, Vertex right) throws RemoteException
    {
        log.info(String.format("getEdge: %s %s\n", left, right));
        int[] fans = fanouts.get(left.getId());
        
        if (fans == null) fans = new int[0];
        
        int index;
        if ((index = Arrays.binarySearch(fans, right.getId())) >= 0){
            return new Edge(left.getId(), fans[index]);
        } else {
            throw new AssertionError("missing edge");
        }
    }

    @Override
    public int[] getFanout(Vertex v, int pageSize, int offset) throws RemoteException
    {     
        log.info(String.format("getFanout: %s, %d, %d\n", v, pageSize, offset));;        
        int[] fullfanout = fanouts.get(v.getId());
        int pos = UtilMethods.getInsertionIndex(fullfanout, offset);
        
        int[] result = Arrays.copyOfRange(fullfanout, pos, Ints.min(new int[]{pos + pageSize, fullfanout.length}));
        
        log.info(String.format("fanout answer: " + Arrays.toString(result)));
        return result;
    }

    @Override
    public int[] getIntersection(Vertex v, Vertex w, int pageSize, int offset) throws RemoteException
    {
        //System.out.printf("getIntersection %s, %s, %d,  %d\n", v, w, pageSize, offset);

        int[] vfanout = fanouts.get(v.getId());
        int[] wfanout = fanouts.get(w.getId());
        
        return UtilMethods.intersectSortedUniqueArraySet(vfanout, wfanout, pageSize, offset);
    }

    public void putFanout(int id, int[] fanouts){
        System.out.println("WARNING: remote put fanout: " +  id);
        this.fanouts.put(id, fanouts);
    }
    
    public void localPutFanout(int id, int[] fanouts){
        //System.out.println("local put fanout: " +  id + "fanouts: " + Arrays.toString(fanouts));
        this.fanouts.put(id, fanouts);
    }
    
    
    @Override
    public void putEdge(Edge e) throws RemoteException
    {
        System.out.println("WARNING: call to putEdge operation. Ignoring.");
    }

    @Override
    public void reset() throws RemoteException
    {
        System.out.println("RESETTING...");
        fanouts.clear();
    }

    @Override
    public int totalLoad() throws RemoteException
    {
        throw new NotImplementedException();
    }
    
    public void finishLoading() {
        throw new NotImplementedException();
    }
}