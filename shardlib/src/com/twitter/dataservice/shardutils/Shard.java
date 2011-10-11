package com.twitter.dataservice.shardutils;

public class Shard implements Comparable<Shard>
{
    int id;
    Token lowerEnd;

  public Shard(Token lowerEnd) {
    this.lowerEnd = lowerEnd;
  }
    //lower end in token space
  public Token getLowerEnd(){
    return lowerEnd;
  }
  
@Override
public int compareTo(Shard o)
{
    return this.lowerEnd.compareTo(o.lowerEnd);
}
 
}
