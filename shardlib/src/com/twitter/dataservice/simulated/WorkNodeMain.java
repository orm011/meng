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
import java.util.Properties;

import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.dataservice.remotes.IDataNode;

public class WorkNodeMain
{
	static int DEFAULT_VERTICES = 10000;
	
    public static void main(String[] argv){    
        if (argv.length == 0){
            System.out.println("usage: NodeName...");
            System.exit(1);    
        }

        String logPropertyFile = "log4j.properties";
        PropertyConfigurator.configure(logPropertyFile);
        
        String benchmarkPropertyFile = "benchmark.properties";
        Properties prop = Benchmark.parsePropertyFile(benchmarkPropertyFile); 
        int workRounds = Integer.parseInt(prop.getProperty("system.workRounds"));
        Logger mylogger = LoggerFactory.getLogger(WorkNodeMain.class);
        mylogger.debug("workRounds {}", workRounds);
        
        if (System.getSecurityManager() == null)
            System.setSecurityManager(new SecurityManager());
        
        try {            
            Registry registry;
            registry = LocateRegistry.createRegistry(1099);
            
            List<IDataNode> nodes = new LinkedList<IDataNode>();
            
            
            for (String namefile: argv){
                String[] nodeparams = namefile.split("-");
            	String name = nodeparams[0];
            	int size = DEFAULT_VERTICES;

                CompactDataNode dn = new BriefDataNode(size, name, workRounds); 
                	//new CompactDataNode(size, name);
                nodes.add(dn);
                IDataNode stub =
                    (IDataNode) UnicastRemoteObject.exportObject(dn, 0);
                System.out.printf("Registering work node %s...\n", name);
                registry.rebind(name, stub);                                
                System.out.println("Success");
            }
            

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