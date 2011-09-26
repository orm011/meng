package com.twitter.dataservice.shardlib;

import java.util.List;

public interface IShard
{
    //returns the size in tokens 
    public int getSize();
 
    //lower end in token space
    public Token getLowerEnd();
    
    //upper end in token space
    public Token getUpperEnd();

    //list of replicas that hold shard
    public List<Node> getReplicas();
}
