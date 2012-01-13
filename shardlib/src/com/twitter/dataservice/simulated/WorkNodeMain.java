package com.twitter.dataservice.simulated;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.LinkedList;
import java.util.List;

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
            
            int x = 5;
            while (x > 0) {
                Thread.sleep(2000);
                System.out.println("starting in ..." +  x);
                x--;
            }
            
            List<IDataNode> nodes = new LinkedList<IDataNode>();
            
            for (String namefile: argv){
                String name = namefile.split("-")[0];
                
                System.out.printf("Registering work node %s...\n", name);
                IDataNode dn = new DictionaryBackedDataNode(name);
                nodes.add(dn);
                IDataNode stub =
                    (IDataNode) UnicastRemoteObject.exportObject(dn, 0);
                registry.rebind(name, stub);
                
                if (namefile.split("-").length > 1){
                    String filename = namefile.split("-")[1];    
                    System.out.println(String.format("loading %s using file %s", name, filename));
                    loadFromLocal(filename, dn);
                }
                
                System.out.println("Great success.");
            }
            
            System.out.println("about to gc");
            System.gc();
            
            while (true) {
                Thread.sleep(5000);
                System.out.println("sleeping..");
            }
            
        } catch (Exception e){
            e.printStackTrace();
            System.exit(1);
        }
  }
    
   public static void loadFromLocal(String fileName, IDataNode node){
       try
    {
        BufferedReader bfr = new BufferedReader(new FileReader(fileName));
        String line;
        while ((line = bfr.readLine()) != null){
            String[] elts = line.split("\t");
            int leftid = Integer.parseInt(elts[1]);
            int rightid = Integer.parseInt(elts[2]);
            
            node.putEdge(new Edge(new Vertex(leftid), new Vertex(rightid)));
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