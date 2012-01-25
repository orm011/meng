package com.twitter.dataservice.simulated;

import java.rmi.RemoteException;
import java.util.Collection;

import com.twitter.dataservice.remotes.IDataNode;
import com.twitter.dataservice.shardutils.Edge;
import com.twitter.dataservice.shardutils.Vertex;

public abstract class AbstractDataNode implements IDataNode
{
    @Override
    abstract public Edge getEdge(Vertex left, Vertex right) throws RemoteException;

    @Override
    abstract public int[] getFanout(Vertex v, int pageSize, int offset) throws RemoteException;
        
    public Collection<Vertex> getFanout(Vertex v) throws RemoteException {
        return UtilMethods.toVertexCollection(getFanout(v, Integer.MAX_VALUE, -1));
    }
    
    @Override
    abstract public int[] getIntersection(Vertex v, Vertex w, int pageSize, int offset) throws RemoteException;
    
    public Collection<Vertex> getIntersection(Vertex v, Vertex w) throws RemoteException {
        return UtilMethods.toVertexCollection(getIntersection(v, w, Integer.MAX_VALUE, -1));
    }
    
    @Override
    abstract public void putEdge(Edge e) throws RemoteException;

    @Override
    abstract public void reset() throws RemoteException;
    
    @Override
    abstract public int totalLoad() throws RemoteException;
}
