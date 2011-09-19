package com.twitter.dataservice.shardlib;

public interface IShard
{
    //returns the size in tokens 
    public int getSize();
 
    //lower end in token space
    public IToken getLowerEnd();
    
    //upper end in token space
    public IToken getUpperEnd();

    //list of replicas that hold shard
    public List<INode> getReplicas();
}
