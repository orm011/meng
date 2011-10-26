package com.twitter.dataservice.simulated;

import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.Map;

import com.twitter.dataservice.remotes.ICheckedFanOutNode;
import com.twitter.dataservice.shardutils.Edge;
import com.twitter.dataservice.shardutils.Vertex;


//TODO: think more about whether I want anything like this, meanwhile
// can use the CompleteWorkNode class
public class CheckedFanOutNode implements ICheckedFanOutNode
{
    Map<Vertex, Integer> internalCheckMap = new HashMap<Vertex, Integer>();
        
    @Override
    public Boolean getEdge(Edge e) throws RemoteException
    {
        return null;
    }

    @Override
    public Integer getNeighbors(Vertex v) throws RemoteException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void putEdge(Edge e) throws RemoteException
    {
        // TODO Auto-generated method stub
        
    }

}
