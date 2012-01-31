package com.twitter.dataservice.shardutils;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;

@SuppressWarnings("serial")
public class Vertex implements IByteSerializable, Serializable
{
    public final static Vertex ZERO = new Vertex(0);
    public final static Vertex ONE = new Vertex(1);
    public final static Vertex TWO = new Vertex(2);

    public static int[] toIntArray(Collection<Vertex> col){
        int[] answer = new int[col.size()];
        
        int pos = 0;
        for (Vertex v : col){
            answer[pos] = v.getId();
            pos++;
        }
        
        return answer;
    }
    
    final int id;

  public int getWorkFactor() {
    return workFactor;
  }

  final int workFactor;
    
    public Vertex(int id, int workFactor){
        this.id = id;
        this.workFactor = workFactor;
    }
    
    public Vertex(int id){
        this.id = id;
        this.workFactor = 0;
    }

    
    public byte[] toByteArray(){
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
       
        try{
            dos.writeUTF(Vertex.class.toString());
            dos.writeInt(id);
        } catch (IOException e) {
            //its a byte array internally, so this is not a typical error
            throw new RuntimeException(e);
        }
        
        return baos.toByteArray();
    }

    public int getId(){
      return id;
    }

  @Override public String toString() {
    return String.format("Vertex: %d", id);
  }

  @Override
public int hashCode()
{
    final int prime = 31;
    int result = 1;
    result = prime * result + id;
    return result;
}


@Override
public boolean equals(Object obj)
{
    if (this == obj)
        return true;
    if (obj == null)
        return false;
    if (getClass() != obj.getClass())
        return false;
    Vertex other = (Vertex) obj;
    if (id != other.id)
        return false;
    return true;
}

}
