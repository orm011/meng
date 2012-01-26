package com.twitter.dataservice.simulated;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import com.google.common.primitives.Ints;
import com.twitter.dataservice.shardutils.Node;
import com.twitter.dataservice.shardutils.Vertex;

public class UtilMethods
{

    public static int getInsertionIndex(int[] array, int key){
        int searchi = Arrays.binarySearch(array, key);
        return (1 - (searchi >>> 31))*searchi + (searchi >>> 31)*(searchi ^ -1);
    }
    
    //assumes sorted & unique
    public static int[] intersectSortedUniqueArraySet(int[] leftarray, int[] rightarray, int pageSize, int offset){
        int i = getInsertionIndex(leftarray, offset);
        int j = getInsertionIndex(rightarray, offset);
            
        int count = 0;
        ArrayList<Integer> result = new ArrayList<Integer>();
        
        //System.out.println(String.format("%d,%d", i,j));
        while (i < leftarray.length && j < rightarray.length && count < pageSize){
            if (leftarray[i] > rightarray[j]){
                j++;
            } else if (leftarray[i] < rightarray[j]){
                i++;
            } else {
                //System.out.println(String.format("%d, %d", i, j));
                result.add(leftarray[i]);
                i++; j++; count++;
            }
        }
                
        return Ints.toArray(result);     
    }
    
    
    public static Collection<Vertex> toVertexCollection(int[] ids){
        Collection<Vertex> wrap = new ArrayList<Vertex>(ids.length);        
        for (int id: ids){
            wrap.add(new Vertex(id));
        }
        return wrap;
    }
    
    public static Collection<Node> toNodeCollection(int[] nodes){
        Collection<Node> wrap = new ArrayList<Node>(nodes.length);
        for (int nodeid: nodes){
            wrap.add(new Node(nodeid));
        }
        return wrap;
    }
    
    public static Collection<Node> toNodeCollection(byte[] nodes){
        Collection<Node> wrap = new ArrayList<Node>(nodes.length);
        for (byte nodeid: nodes){
            wrap.add(new Node(nodeid));
        }
        return wrap;
    }
    
}
