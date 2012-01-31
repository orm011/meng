package com.twitter.dataservice.simulated;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.PropertyConfigurator;

import com.google.common.base.Ticker;
import com.twitter.dataservice.remotes.IDataNode;
import com.twitter.dataservice.shardutils.Edge;
import com.twitter.dataservice.shardutils.Vertex;

public class WorkNodeMain
{
    public static void main(String[] argv){
        
        if (argv.length == 0){
            System.out.println("usage: NodeName-[loadFile] [NodeName-[loadFile]...] ");
            System.exit(1);    
        }
                
        if (System.getSecurityManager() == null)
            System.setSecurityManager(new SecurityManager());
        
        try {            
            Registry registry;
            registry = LocateRegistry.createRegistry(1099);
            
            List<IDataNode> nodes = new LinkedList<IDataNode>();
         
            Ticker t = Ticker.systemTicker();
            long start = t.read();
            for (String namefile: argv){
                String name = namefile.split("-")[0];
                
                System.out.printf("Registering work node %s...\n", name);
                CompactDataNode dn = new CompactDataNode();
                nodes.add(dn);
                IDataNode stub =
                    (IDataNode) UnicastRemoteObject.exportObject(dn, 0);
                registry.rebind(name, stub);
                
                if (namefile.split("-").length > 1){
                    String filename = namefile.split("-")[1];    
                    System.out.println(String.format("loading %s using local file %s", name, filename));
                    loadFromLocal(filename, dn);
                }
                
                System.out.println("Success");
            }
            
            System.out.println("loading done. time: " + (t.read() - start));
//            while (true) {
//                Thread.sleep(5000);
//                System.out.println("sleeping..");
//            }
//            
        } catch (Exception e){
            e.printStackTrace();
            System.exit(1);
        }
  }
    
    /*
     * only available for CompactDataNode because we put fanout arrays directly into node.
     */
   public static void loadFromLocal(String fileName, CompactDataNode node){
       try
    {
        BufferedReader bfr = new BufferedReader(new FileReader(fileName));
        String line;
        while ((line = bfr.readLine()) != null){
            String[] elts = line.split("\t");
            int leftid = Integer.parseInt(elts[0]);
            
            //System.out.println(leftid);
            //System.out.println(Arrays.toString(elts));
            int[] fanouts = new int[elts.length - 1];
            int i;
            for (i = 0; i < fanouts.length; i++){
                fanouts[i] = Integer.parseInt(elts[i+1]);
            }
            
            //System.out.println("putting fanout: " + leftid + " " + Arrays.toString(fanouts));
            node.localPutFanout(leftid, fanouts);
        }  
    } catch (FileNotFoundException e)
    {
        throw new RuntimeException(e);
    } catch (IOException e)
    {
        throw new RuntimeException(e);
    }
    
  }

}