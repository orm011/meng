package com.twitter.dataservice.shardutils;

import java.io.ByteArrayOutputStream;

public class Edge
{
    Pair<Vertex, Vertex> ends;    
    
    public Edge(Vertex left, Vertex right){
        ends = new Pair<Vertex, Vertex>(left, right);
    }    
    
    public Vertex getLeftEndpoint(){
        return ends.getLeft();
    }
    
    public Vertex getRightEndpoint(){
        return ends.getRight();
    }
    
    public byte[] toByteArray(){
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        byte[] left = getLeftEndpoint().toByteArray();
        byte[] right = getRightEndpoint().toByteArray();
        baos.write(left, 0, left.length);
        baos.write(right, 0, right.length);
        return baos.toByteArray();
    }
}
