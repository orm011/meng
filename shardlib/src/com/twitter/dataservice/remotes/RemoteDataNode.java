package com.twitter.dataservice.remotes;

import java.rmi.Remote;

//operations at node level
public interface RemoteDataNode extends Remote
{
    byte[] getEdge() throws java.rmi.RemoteException;

    //the double returns some paramter of interest to the caller such as the nanotime diff.
    //so that communication times can be inferred from the difference
    byte[] getNeighbors(Integer workFactor) throws java.rmi.RemoteException;
    
    //may want to use to emphasize the benefits of pushing intersection to nodes
    byte[] getIntersection(Integer workFactorLeft, Integer workFactorRight, float intersectionFactor) throws java.rmi.RemoteException; 
}


