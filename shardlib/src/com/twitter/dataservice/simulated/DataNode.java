package com.twitter.dataservice.simulated;

import java.net.Inet4Address;
import java.rmi.Naming;
import java.rmi.RMISecurityManager;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import com.twitter.dataservice.remotes.RemoteDataNode;

public class DataNode extends UnicastRemoteObject implements RemoteDataNode 
{
    
    //has a: executor for incoming workTasks
    ExecutorService workExecutor;
    
    public DataNode(int numWorkers) throws RemoteException {
        workExecutor = Executors.newFixedThreadPool(numWorkers);
    }

    @Override
    public byte[] getEdge() throws RemoteException
    {
        workExecutor.execute(new WorkTask(1));
        return new byte[]{0};
    }

    @Override
    public byte[] getIntersection(Integer workFactorLeft, Integer workFactorRight, float intersectionFactor)
            throws RemoteException
    {
        //TODO: make the byte array larger
        workExecutor.execute(new WorkTask(workFactorLeft*workFactorRight));
        return new byte[]{0};
    }

    @Override
    public byte[] getNeighbors(Integer workFactor) throws RemoteException
    {
        workExecutor.execute(new WorkTask(workFactor));
        return new byte[]{0};
    }
    
    public static void main(String[] argv){
        if (System.getSecurityManager() == null)
            System.setSecurityManager(new RMISecurityManager());
        
        //figure out if the host name in use makes sense
        
        try{
            DataNode dn = new DataNode(6);
            Naming.rebind("/DataNodeServer", dn);
            System.out.println("successfully bound");
        } catch (Exception e){
            e.printStackTrace();
            System.out.println(e.getMessage());
            System.exit(1);
        }
    }
    
}
