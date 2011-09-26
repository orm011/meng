package com.twitter.dataservice.simulated;

import com.twitter.dataservice.remotes.RemoteDataNode;
import com.twitter.dataservice.sharding.ISharding;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class APIServer
{
    private ISharding shardinglib = null;
    
    List<RemoteDataNode> datanodes = new LinkedList<RemoteDataNode>();
    
    public APIServer(String[] dataNodeNames) {
        
        try {
            for (String name: dataNodeNames){
                System.out.printf("looking up node %s\n", name);
                datanodes.add((RemoteDataNode) Naming.lookup(name));
            }
        } catch (RemoteException e) {
            System.out.println("failed to find remote object: " + e.getMessage());
        } catch (NotBoundException e) {
            e.printStackTrace();
        } catch (MalformedURLException e){
            e.printStackTrace();
        }
    }
    
//
//    public List<String> getAllEdges(int workFactor){
//
//    }
    
    public List<String> getEdge(){
        List<byte[]> results = new ArrayList<byte[]>(5);
        List<String> successes = new ArrayList<String>(5);
        for (RemoteDataNode node: datanodes){
                try
                {
                    results.add(node.getEdge());
                    successes.add(node.toString());
                } catch (RemoteException e)
                {
                    e.printStackTrace();
                }
        }
        
        return successes;
    }
    
    public static void main(String[] args){
        APIServer api = new APIServer(args);
        List<String> successes = api.getEdge();
        
        for (String name : successes)
            System.out.println(name);
    }

}
    