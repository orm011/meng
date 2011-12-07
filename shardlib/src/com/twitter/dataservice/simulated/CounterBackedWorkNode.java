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
import java.util.concurrent.atomic.AtomicInteger;

import com.twitter.dataservice.remotes.IDataNode;
import com.twitter.dataservice.shardutils.Edge;
import com.twitter.dataservice.shardutils.Vertex;

public class CounterBackedWorkNode implements IDataNode
{   
    //TODO: will need to keep a dictionary or so to store data
    Counter<Vertex> internalCount = new MapBackedCounter<Vertex>();
    

    @Override
    public Edge getEdge(Vertex left, Vertex right) throws RemoteException
    {
        //we don't check if it is there, to not have to keep track.
        //we assume the shardling lib did a good job of directing the request.
        //edges have a payload, check parameters in systemparams.
        if (!(internalCount.getCount(left) > 0)) throw new AssertionError();
        //System.out.println("Edge request");
        return new Edge(left, right);
    }

    @Override
    public Collection<Vertex> getFanout(Vertex v) throws RemoteException
    {
        //System.out.println("FanOut request " + v);
        int x = 0;
        
        if ( !((x = internalCount.getCount(v)) > 0)) throw new AssertionError();

        ArrayList<Vertex> answer = new ArrayList<Vertex>(x);        
        for (int i = 0; i < x; i++){
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
        internalCount.increaseCount(e.getLeftEndpoint());
    }

    @Override
    public void reset() throws RemoteException
    {
        System.out.printf("resetting... before-count: %d", internalCount.getTotal());
        internalCount = new MapBackedCounter<Vertex>();
        System.out.printf(" after-count: %d\n", internalCount.getTotal());
    }

    @Override
    public int totalLoad() throws RemoteException
    {
        return internalCount.getTotal();
    }
    
}