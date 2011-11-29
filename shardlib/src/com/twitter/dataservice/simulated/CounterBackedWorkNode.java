package com.twitter.dataservice.simulated;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.twitter.dataservice.remotes.ICompleteWorkNode;
import com.twitter.dataservice.shardutils.Edge;
import com.twitter.dataservice.shardutils.Vertex;

public class CounterBackedWorkNode  extends UnicastRemoteObject implements ICompleteWorkNode
{   
    //TODO: will need to keep a dictionary or so to store data
    Counter<Vertex> internalCount = new MapBackedCounter<Vertex>();
    
    public CounterBackedWorkNode() throws RemoteException
    {
        super();
    }

    @Override
    public Edge getEdge(Vertex left, Vertex right) throws RemoteException
    {
        //we don't check if it is there, to not have to keep track.
        //we assume the shardling lib did a good job of directing the request.
        //edges have a payload, check parameters in systemparams.
        assert internalCount.getCount(left) > 0;
        //System.out.println("Edge request");
        return new Edge(left, right);
    }

    @Override
    public Collection<Vertex> getFanout(Vertex v) throws RemoteException
    {
        //System.out.println("FanOut request");
        int x = 0;
        
        assert (x = internalCount.getCount(v)) > 0;
        ArrayList<Vertex> answer = new ArrayList<Vertex>(x);        
        for (int i = 0; i < answer.size(); i++){
            answer.add(i, new Vertex(i));
        }
        
        return answer;
    }

    @Override
    public List<Vertex> getIntersection(Vertex v, Vertex w) throws RemoteException
    {
        //System.out.println("Intersection request");
        throw new UnsupportedOperationException();
    }

    @Override
    public void putEdge(Edge e) throws RemoteException
    {
        //System.out.println("put request");
        internalCount.increaseCount(e.getLeftEndpoint());
    }

}