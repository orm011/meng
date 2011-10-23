package com.twitter.dataservice.shardutils;

public class Shard implements Comparable<Shard>
{
    int id;
    Token upperEndToken;

  public Shard(Token upperEndToken) {
    this.upperEndToken = upperEndToken;
  }
    //lower end in token space
  public Token getUpperEndToken(){
    return upperEndToken;
  }
  
@Override
public int compareTo(Shard o)
{
    return this.upperEndToken.compareTo(o.upperEndToken);
}

@Override
public String toString(){
    return "Shard: " + getUpperEndToken().toString();
}

@Override
public boolean equals(Object o){
    return (o != null) && (o instanceof Shard) && upperEndToken.equals(((Shard)o).upperEndToken);
}
 
}
