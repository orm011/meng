package com.twitter.dataservice.shardlib;

import java.io.ByteArrayOutputStream;

public class Vertex
{
    final int id;

  public int getWorkFactor() {
    return workFactor;
  }

  final int workFactor;
    
    public Vertex(int id, int workFactor){
        this.id = id;
        this.workFactor = workFactor;
    }

    
    public byte[] toByteArray(){
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
      baos.write(id);
        return baos.toByteArray();
    }

  @Override public String toString() {
    return String.format("Vertex: id: %d, workFactor: %d", id, getWorkFactor());
  }

  @Override public boolean equals(Object o) {
    return (o instanceof Vertex) && (((Vertex)o).id == id);
  }

}
