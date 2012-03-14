package com.twitter.dataservice.shardutils;

import java.io.ByteArrayOutputStream;
import java.io.Serializable;

import com.twitter.dataservice.parameters.SystemParameters;

public class Edge implements Serializable
{
	public static final int DEFAULT_EDGE_WEIGHT = 1;
    Pair<Vertex, Vertex> ends;    
    Vertex left;
    Vertex right;
    public byte[] payload = new byte[Edge.DEFAULT_EDGE_WEIGHT];
    
    public Edge(Vertex left, Vertex right){
        this.left = left;
        this.right = right;
    }
    
    public Edge(Integer left, Integer right, byte[] metadata){
        this.left = new Vertex(left);
        this.right = new Vertex(right);
        this.payload = metadata;
    }
    
    public Edge(int leftid, int rightid){
        this.left = new Vertex(leftid);
        this.right = new Vertex(rightid);
    }
    
    public Vertex getLeftEndpoint(){
        return left;
    }
    
    public Vertex getRightEndpoint(){
        return right;
    }
    
    @Override
    public boolean equals(Object o){
        return o instanceof Edge && ((Edge) o).getLeftEndpoint().equals(this.getLeftEndpoint()) &&
        ((Edge) o).getRightEndpoint().equals(this.getRightEndpoint());
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