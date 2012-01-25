package com.twitter.dataservice.simulated;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;


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
    //consistency condition: there is an edge (u, v) for every v in fanout(u)
    //note the edge object also contains the pair.    
    private HashMap<Integer, int[]> fanouts;
    private final String name;   
    
    public CompactDataNode(int numVertices){
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
        //TODO: get rid of binary search, just return 'yes'
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
        System.out.println("fanout: " + v);
        
        int[] fullfanout = fanouts.get(v.getId());
        int pos = UtilMethods.getInsertionIndex(fullfanout, offset);
        
        int[] result = Arrays.copyOfRange(fullfanout, pos, Ints.min(new int[]{pos + pageSize, fullfanout.length}));
        return result;
    }

    @Override
    public int[] getIntersection(Vertex v, Vertex w, int pageSize, int offset) throws RemoteException
    {
        System.out.println("getIntersection");

        int[] vfanout = fanouts.get(v.getId());
        int[] wfanout = fanouts.get(w.getId());
        
        return UtilMethods.intersectSortedUniqueArraySet(vfanout, wfanout, pageSize, offset);
    }

    public void putFanout(int id, int[] fanouts){
        this.fanouts.put(id, fanouts);
    }
    
    @Override
    public void putEdge(Edge e) throws RemoteException
    {
        throw new NotImplementedException();        
    }

    @Override
    public void reset() throws RemoteException
    {
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