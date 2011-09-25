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
    String name;
    //has a: executor for incoming workTasks
    ExecutorService workExecutor;
    
    public DataNode(int numWorkers, String name) throws RemoteException {
        this.name = name;
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

        String name  = argv[0];
        if (System.getSecurityManager() == null)
            System.setSecurityManager(new RMISecurityManager());
        
        //figure out if the host name in use makes sense
        
        try{
            System.out.printf("registering work node as: %s\n", name);
            DataNode dn = new DataNode(SystemParameters.workersPerNode, name);
            Naming.rebind(dn.name, dn);
            System.out.println("successfully bound");
        } catch (Exception e){
            e.printStackTrace();
            System.exit(1);
        }
    }
    
}
