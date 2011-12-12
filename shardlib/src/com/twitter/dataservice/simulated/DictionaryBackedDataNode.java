package com.twitter.dataservice.simulated;

import java.rmi.RemoteException;
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
public class DictionaryBackedDataNode implements IDataNode
{
    //consistency condition: there is an edge (u, v) for every v in fanout(u)
    //note the edge object also contains the pair.
    private final Map<Pair<Vertex, Vertex>, Edge> edges = new HashMap<Pair<Vertex, Vertex>, Edge>();
    private final Map<Vertex, Set<Vertex>> fanouts = new HashMap<Vertex, Set<Vertex>>();
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
        Edge ans = edges.get(new Pair<Vertex, Vertex>(left, right));
        if (ans == null) throw new AssertionError();
        return ans;
    }

    @Override //TODO: how does RMI choose the type of collection, if Collection is abstract?
    //I guess it must be the actual type. In that case, may want to make a list?
    public Collection<Vertex> getFanout(Vertex v) throws RemoteException
    {
        //System.out.println(name + " fanout: " + v);
        Collection<Vertex> temp = fanouts.get(v);
        Collection<Vertex> ans = (temp == null) ? new LinkedList<Vertex>() : temp;
        
        //not really true: when a node only has a few edges, one of the shard containing 
        //nodes may have no entries for it, while some other shard nodes do have it.
        //if (ans == null) ans = new ArrayList<Vertex>();
        return ans;
    }

    @Override
    public List<Vertex> getIntersection(Vertex v, Vertex w) throws RemoteException
    {
        throw new NotImplementedException();
    }

    @Override
    public void putEdge(Edge e) throws RemoteException
    {
        //System.out.println(name + " put: " + e);
        Vertex left = e.getLeftEndpoint();
        Vertex right = e.getRightEndpoint();
        
        Pair<Vertex, Vertex> p = new Pair<Vertex, Vertex>(left, right);
        
        //update edges structure
        assert !edges.containsKey(p);
        edges.put(p, e);
        
        //update fanout structure
        if (!fanouts.containsKey(left)){
            fanouts.put(left, new HashSet<Vertex>());
        }
        
        fanouts.get(left).add(right);        
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
}
