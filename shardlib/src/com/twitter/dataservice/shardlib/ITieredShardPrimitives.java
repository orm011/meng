package com.twitter.dataservice.shardlib;

import java.util.List;

public interface ITieredShardPrimitives extends IShardPrimitives
{
    //in addition to the other ones you saw.
    //these could be useful methods from a shard state object from a tiered-approach perspective. but maybe too specific.
    //The IKey is the actual exception, and the Int indicates a tier (useful when we want more than 2 tiers)
    //Also, how would you store the shards? would it be simply a list of delimiter tokens, or would it be  a list of objects made up 
    // of 2 delimiter tokens (specifying the two endpoints. It may seem redundant, but I can use that in my own ways, eg to define shards that seem to overlap based purely on endpoints).
    public List<Pair<IKey, Integer>> getExceptionalKeys();
    public List<IShard> getShardsForTier(Integer tierNumber);
    public List<IShard> putExceptionalKeys();
    public List<IShard> putShardForTier();    
}

public interface ILookupShardPrimitives extends IShardPrimitives
{
    //here I am trying to less tier-centric, but still assume that the vast majority of keys will be dealt with in 
    //the default way, and a fancier strategy applies to some other keys.   
    public List<Pair<IKey, Integer>> getExceptionKeys();
    public void putExceptionKeys();
}

public interface ILookupGeneralShardPrimitive extends IShardPrimitives
{
    //or why not just this?
    public byte[] getCustomData();
    public byte[] putCustomData();    
}

