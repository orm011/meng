package com.twitter.dataservice.shardlib;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.List;

public class Shard
{
    Token lowerEnd;
    Token upperEnd;
    //returns the size in tokens

  public Shard(Token lowerEnd, Token upperEnd) {
    this.lowerEnd = lowerEnd;
    this.upperEnd = upperEnd;
  }

    public int getSize(){
      throw new NotImplementedException();
    }
 
    //lower end in token space
    public Token getLowerEnd(){
      return lowerEnd;
    }
    
    //upper end in token space
    public Token getUpperEnd(){
      return upperEnd;
    }

    //list of replicas that hold shard
    public List<Node> getReplicas(){
      throw new NotImplementedException();
    }
}
