package com.twitter.dataservice.shardlib;

import java.util.List;
import java.util.Set;

//primitives as understood from Stu's description
public interface IShardPrimitives
{
    public List<IShard> getShardList();
    
    public void setShardList(List<IShard> shards);
    
    public Set<INode> getReplicaSet(IShard shard);
    
    public void setReplicaSet(IShard s, Set<INode> replica);
}
