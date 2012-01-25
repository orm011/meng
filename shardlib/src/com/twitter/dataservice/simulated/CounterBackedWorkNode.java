package com.twitter.dataservice.simulated;

import java.rmi.RemoteException;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import com.twitter.dataservice.shardutils.Edge;
import com.twitter.dataservice.shardutils.Vertex;


public class CounterBackedWorkNode extends AbstractDataNode
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
    public int[] getFanout(Vertex v, int pageSize, int offset) throws RemoteException
    {
        if (pageSize != Integer.MAX_VALUE || !(offset < 0)) throw new NotImplementedException();
        //System.out.println("FanOut request " + v);
        int x = 0;        
        if ( !((x = internalCount.getCount(v)) > 0)) throw new AssertionError();

        int[] ans = new int[x];
        for (int i = 0; i < x; i++){
            ans[i] = i;
        }

        return ans;
    }

    @Override
    public int[] getIntersection(Vertex v, Vertex w, int pageSize, int offset) throws RemoteException
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

    @Override
    public void putFanout(int vertex, int[] fanout)
    {
        // TODO Auto-generated method stub
       throw new NotImplementedException(); 
    }
    
}