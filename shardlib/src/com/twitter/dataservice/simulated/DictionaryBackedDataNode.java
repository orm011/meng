package com.twitter.dataservice.simulated;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import com.twitter.dataservice.remotes.IDataNode;
import com.twitter.dataservice.shardutils.Edge;
import com.twitter.dataservice.shardutils.Pair;
import com.twitter.dataservice.shardutils.Vertex;

/*
 * oscarm's version of redis. 
 * TODO: how do we make sure there are enough buckets in the hashtable?
 */
public class DictionaryBackedDataNode extends AbstractDataNode implements IDataNode
{
    //consistency condition: there is an edge (u, v) for every v in fanout(u)
    //note the edge object also contains the pair.
    private final Map<Pair<Integer, Integer>, byte[]> edges = new HashMap<Pair<Integer, Integer>, byte[]>();
    private final Map<Integer, List<Integer>> fanouts = new HashMap<Integer, List<Integer>>();
    //TODO: deal with backedges. add a funelin data structure?
    private final String name;
    
    public DictionaryBackedDataNode(String name){
        this.name = name;
    }
    
    public DictionaryBackedDataNode(){
        this.name = null;
    }
    
    
    @Override
    public Edge getEdge(Vertex left, Vertex right) throws RemoteException
    {
        Edge ans = new Edge(left.getId(), right.getId(), edges.get(new Pair<Integer, Integer>(left.getId(), right.getId())));
        if (ans == null) throw new AssertionError();
        return ans;
    }
    
    @Override
    public int[] getFanout(Vertex v, int pageSize, int offset){
        throw new NotImplementedException();
    }

    @Override
    public Collection<Vertex> getFanout(Vertex v) throws RemoteException
    {
        
        System.out.println("fanout: " + v);
        Collection<Integer> out = fanouts.get(v.getId());
        
        Collection<Vertex> temp = new ArrayList<Vertex>();
        
        for (Integer id: out){
            System.out.println(id);
            temp.add(new Vertex(id));
        }
        
        return temp;
    }
    
    @Override
    public int[] getIntersection(Vertex v, Vertex w, int pageSize, int offset){
        throw new NotImplementedException();
    }

    @Override
    public List<Vertex> getIntersection(Vertex v, Vertex w) throws RemoteException
    {
        throw new NotImplementedException();
    }

    @Override
    public void putEdge(Edge e) throws RemoteException
    {
        System.out.println(name + " put: " + e);
        Vertex left = e.getLeftEndpoint();
        Vertex right = e.getRightEndpoint();
        
        Pair<Integer, Integer> p = new Pair<Integer, Integer>(left.getId(), right.getId());
        
        //update edges structure
        assert !edges.containsKey(p);
        edges.put(p, e.payload);
        
        //update fanout structure
        if (!fanouts.containsKey(left.getId())){
            fanouts.put(left.getId(), new ArrayList<Integer>());
        }
        
        fanouts.get(left.getId()).add(right.getId());        
    }

    @Override
    public void reset() throws RemoteException
    {
        edges.clear();
        fanouts.clear();
    }

    @Override
    public int totalLoad() throws RemoteException
    {
        return edges.size();
    }

    @Override
    public void putFanout(int vertex, int[] fanout)
    {
        // TODO Auto-generated method stub
        throw new NotImplementedException();
    }
}
