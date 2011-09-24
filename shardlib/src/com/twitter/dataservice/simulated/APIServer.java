package com.twitter.dataservice.simulated;

import java.net.Inet4Address;
import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;

import com.twitter.dataservice.remotes.RemoteDataNode;
import com.twitter.dataservice.sharding.ISharding;

public class APIServer
{
    private ISharding shardinglib = null;
    
    RemoteDataNode rn;
    
    public APIServer() {
        try {
            rn = (RemoteDataNode) Naming.lookup("/DataNodeServer");
        } catch (RemoteException e) {
            System.out.println("failed to find remote object: " + e.getMessage());
        } catch (NotBoundException e) {
            e.printStackTrace();
        } catch (MalformedURLException e){
            e.printStackTrace();
        }
    }
    
    public byte[] getEdge(){
        try
        {
            return rn.getEdge();
        } catch (RemoteException e)
        {
            e.printStackTrace();
            return new byte[]{(byte) 0xff};
        }
    }
    
    public static void main(String args[]){
        APIServer api = new APIServer();
        byte[] a = api.getEdge();
        System.out.println(String.format("Size: %d Elt: %s", a.length, a[0]));
    }

}
    