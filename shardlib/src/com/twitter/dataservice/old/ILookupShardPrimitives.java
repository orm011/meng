package com.twitter.dataservice.old;

import com.twitter.dataservice.sharding.IShardPrimitives;
import com.twitter.dataservice.shardutils.Vertex;

import java.util.List;


public interface ILookupShardPrimitives extends IShardPrimitives
{
    //here I am trying to less tier-centric, but still assume that the vast majority of keys will be dealt with in 
    //the default way, and a fancier strategy applies to some other keys.   
    public List<Vertex> getExceptionKeys();
    public void putExceptionKeys();
}