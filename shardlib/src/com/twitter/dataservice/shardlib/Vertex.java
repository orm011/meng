package com.twitter.dataservice.shardlib;

import java.io.ByteArrayOutputStream;

public class Vertex
{
    int id;
    
    public Vertex(int id){
        this.id = id;   
    }
    
    public byte[] toByteArray(){
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
      baos.write(id);
        return baos.toByteArray();
    }

  @Override public boolean equals(Object o) {
    return (o instanceof Vertex) && (((Vertex)o).id == id);
  }

}
