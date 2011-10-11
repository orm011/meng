package com.twitter.dataservice.shardutils;

import java.io.ByteArrayOutputStream;

public class Node implements Hashable
{
  int nodeNumber;

  public Node(int number){
    this.nodeNumber = number;
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
