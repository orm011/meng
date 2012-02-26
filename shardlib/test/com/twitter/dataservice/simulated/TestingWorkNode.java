package com.twitter.dataservice.simulated;

import java.rmi.RemoteException;
import java.util.Collection;
import java.util.List;

import org.apache.commons.lang.NotImplementedException;

import com.twitter.dataservice.remotes.IDataNode;
import com.twitter.dataservice.shardutils.Edge;
import com.twitter.dataservice.shardutils.Vertex;

/*
 * used to check all calls are being made
 * and to plot distribution of edges across nodes
 * assumes no concurrent clients for now
 */

public class TestingWorkNode extends AbstractDataNode implements IDataNode
{
    private Counter<Class> counter = new MapBackedCounter<Class>(0);
    private int numEdges = 0;
    
    @Override
    public Edge getEdge(Vertex left, Vertex right) throws RemoteException
    {
        counter.increaseCount(Query.EdgeQuery.class);
        return null;
    }

    @Override
    public Collection<Vertex> getFanout(Vertex v) throws RemoteException
    {
        counter.increaseCount(Query.FanoutQuery.class);
        return null;
    }

    @Override
    public List<Vertex> getIntersection(Vertex v, Vertex w) throws RemoteException
    {
        counter.increaseCount(Query.IntersectionQuery.class);
        return null;
    }

    @Override
    public void putEdge(Edge e) throws RemoteException
    {
        numEdges++;
    }
    
    public Counter<Class> getSummary(){
        return counter;
    }
    
    public int getNumEdges(){
        return numEdges;
    }

    @Override
    public void reset() throws RemoteException
    {
        numEdges = 0;
    }

    @Override
    public int totalLoad() throws RemoteException
    {
        return getNumEdges();
    }

    @Override
    public int[] getFanout(Vertex v, int pageSize, int offset) throws RemoteException
    {
        this.getFanout(v);
        return null;
    }

    @Override
    public int[] getIntersection(Vertex v, Vertex w, int pageSize, int offset) throws RemoteException
    {
        this.getIntersection(v, w);
        return null;
    }

    @Override
    public void putFanout(int vertex, int[] fanout)
    {
        throw new NotImplementedException();
        
    }
}
