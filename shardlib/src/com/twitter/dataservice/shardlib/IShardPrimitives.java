package com.twitter.dataservice.shardlib;

import java.util.List;
import java.util.Set;

//primitives as understood from Stu's description
public interface IShardPrimitives
{
    public List<Shard> getShardList();
    
    public void setShardList(List<Shard> shards);
    
    public Set<Node> getReplicaSet(Shard shard);
    
    public void setReplicaSet(Shard s, Set<Node> replica);
}
