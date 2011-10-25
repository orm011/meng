package com.twitter.dataservice.shardutils;

import java.io.ByteArrayOutputStream;
import java.io.Serializable;

public class  Edge implements Serializable
{
    Pair<Vertex, Vertex> ends;    
    Vertex left;
    Vertex right;
    
    public Edge(Vertex left, Vertex right){
        this.left = left;
        this.right = right;
    }    
    
    public Vertex getLeftEndpoint(){
        return left;
    }
    
    public Vertex getRightEndpoint(){
        return right;
    }
    
    
    //used only by a function if it were to hash each vertex as a whole
    public byte[] toByteArray(){
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        byte[] left = getLeftEndpoint().toByteArray();
        byte[] right = getRightEndpoint().toByteArray();
        baos.write(left, 0, left.length);
        baos.write(right, left.length, right.length);
        return baos.toByteArray();
    }
    
    public String toString(){
        return "Edge: (" + getLeftEndpoint().toString()  + ", " + getRightEndpoint().toString() + ")";
    }
}