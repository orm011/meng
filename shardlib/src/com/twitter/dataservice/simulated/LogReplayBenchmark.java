package com.twitter.dataservice.simulated;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Random;

import com.twitter.dataservice.shardutils.Vertex;
 

public class LogReplayBenchmark implements Iterator<Query>
{
    String logname;
    BufferedReader br;    
    String currentLine = null;
    boolean checkedHasNext = false;
    long count = 0;
    long numqueries = 0;
    static Random randomgen = new Random();

    
    public LogReplayBenchmark(String logname, long numqueries)  {
        this.logname = logname;
        this.numqueries = numqueries;

        try
        {
            this.br = new BufferedReader(new FileReader(logname));
        } catch (FileNotFoundException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean hasNext()
    {
        //count limit
        if (count == numqueries) return false; 
        
        //else do work
        if (checkedHasNext){
            return currentLine != null;
        } else {
            try {
                currentLine = br.readLine();
            } catch (IOException ioe){
                throw new RuntimeException(ioe);
            }
            
            checkedHasNext = true;
            return hasNext();
        }
    }

    @Override
    public Query next()
    {
        if (hasNext()){
            count++;
            checkedHasNext = false;
            
            Query ans = parseQuery(currentLine);
            
            return ans;
        } else {
            throw new NoSuchElementException();
        }
    }

    
    private static String EDGE = "Edge";
    private static String FANOUT = "SimpleQuery";
    private static String INTERSECTION = "SimpleQuery_SimpleQuery_Intersection";

// three accepted examples:
//    4384 Edge 49145792 51016019
//    7 SimpleQuery 500,-1 39009779
//    508 SimpleQuery_SimpleQuery_Intersection 4000,-1 145397885 145397885

// three ignored examples:
//    2043 SimpleQuery_SimpleQuery_Intersection_SimpleQuery_Difference 1000,-1 52427532 52427532 52427532
//    112 SimpleQuery_SimpleQuery_Union 1,-1 172212633 172212633
//    0 LogStart

    
    public static Query parseQuery(String currentLine)
    {
        String[] tokens = currentLine.split("[\t| |,]");
        
        if (tokens.length < 2) {
            return new Query.NopQuery(currentLine);
        } 
            
        String operation = tokens[1];
        
        try {
            //TODO: make it not completely fail upon finding a request makes no sense?
            if (operation.equals(EDGE)){
                Vertex arga = new Vertex(Integer.parseInt(tokens[tokens.length - 2]));
                Vertex argb = new Vertex(Integer.parseInt(tokens[tokens.length - 1]));

                return new Query.EdgeQuery(arga, argb);
            } else if (operation.equals(FANOUT)){
                int pageSize = Integer.parseInt(tokens[tokens.length - 3]);            
                int offset = interpretOffset(Long.parseLong(tokens[tokens.length - 2]));
                Vertex arg = new Vertex(Integer.parseInt(tokens[tokens.length - 1]));

                return new Query.FanoutQuery(arg, pageSize, offset);
            } else if (operation.equals(INTERSECTION)){
                int pageSize = Integer.parseInt(tokens[tokens.length - 4]);
                int offset = interpretOffset(Long.parseLong(tokens[tokens.length - 3]));
                Vertex arga = new Vertex(Integer.parseInt(tokens[tokens.length - 2]));
                Vertex argb = new Vertex(Integer.parseInt(tokens[tokens.length - 1]));
    
                return new Query.IntersectionQuery(arga, argb, pageSize, offset);
            }

        } catch (NumberFormatException ne){
            return new Query.NopQuery("Parse error with line:" + currentLine);
        }
        
        return new Query.NopQuery(currentLine);
    }

    private static int interpretOffset(long flockdbOffset)
    {
        //id space is at a max of < 190 000 000;
        if (flockdbOffset == -1L) return -1;
        return randomgen.nextInt(190000000);
    }

    @Override
    public void remove()
    {
        throw new UnsupportedOperationException();
    }
}
