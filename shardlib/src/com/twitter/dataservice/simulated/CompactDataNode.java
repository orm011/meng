package com.twitter.dataservice.simulated;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;


import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import com.twitter.dataservice.remotes.IDataNode;
import com.twitter.dataservice.shardutils.Edge;
import com.twitter.dataservice.shardutils.Vertex;

/*
 * TODO: how do we make sure there are enough buckets in the hashtable?
 * 
 * */
public class CompactDataNode implements IDataNode
{
    //consistency condition: there is an edge (u, v) for every v in fanout(u)
    //note the edge object also contains the pair.    
    private HashMap<Integer, int[]> fanouts;
    private final String name;   
    
    public CompactDataNode(int numVertices){
        fanouts = new HashMap<Integer, int[]>(numVertices);
        name = "defaultName";
    }
    
    public CompactDataNode(String name){
        this.name = name;
    }
    
    public CompactDataNode(){
        this.name = null;
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
    public Collection<Vertex> getFanout(Vertex v) throws RemoteException
    {        
        System.out.println("fanout: " + v);
        int[] out = fanouts.get(v.getId());
        
        ArrayList<Vertex> temp = new ArrayList<Vertex>(out.length);
        
        for (Integer id: out){
            temp.add(new Vertex(id));
        }
        
        return temp;
    }

    @Override
    public List<Vertex> getIntersection(Vertex v, Vertex w) throws RemoteException
    {
        throw new NotImplementedException();
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