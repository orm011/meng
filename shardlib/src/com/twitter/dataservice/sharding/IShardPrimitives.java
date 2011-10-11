package com.twitter.dataservice.sharding;

import com.twitter.dataservice.shardutils.Node;
import com.twitter.dataservice.shardutils.Shard;
import com.twitter.dataservice.shardutils.Token;

import java.util.List;
import java.util.Set;

//primitives as understood from Stu's description
public interface IShardPrimitives
{
    public List<Token> getShardList();
    
    public void setShardList(List<Shard> shards);
    
    public Set<Node> getReplicaSet(Shard shard);
    
    public void setReplicaSet(Shard s, Set<Node> replica);
}
