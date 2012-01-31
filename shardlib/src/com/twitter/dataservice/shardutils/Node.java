package com.twitter.dataservice.shardutils;

import java.io.ByteArrayOutputStream;

public class Node implements IByteSerializable{

  public final static Node ZERO = new Node(0);    
  public final static Node ONE = new Node(1);
  
  int nodeNumber;

  public static Node getNode(int i){
      return new Node(i);
  }
  
  public Node(int number){
    this.nodeNumber = number;
  }
  
  public int getId(){
      return this.nodeNumber;
  }

  @Override public boolean equals(Object o) {
    return (o instanceof Node) && ((Node) o).nodeNumber == nodeNumber;
  }

  @Override public int hashCode() {
    return new Integer(nodeNumber).hashCode();
  }

  @Override public String toString() {
    return String.format("Node: %x", nodeNumber);
  }
  
   public byte[] toByteArray(){
       ByteArrayOutputStream baos = new ByteArrayOutputStream();
       baos.write(nodeNumber);
       return baos.toByteArray();
  }
}
