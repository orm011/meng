package com.twitter.dataservice.shardutils;

public class Node
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
}
