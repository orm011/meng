package com.twitter.dataservice.simulated;

import java.util.Map;


//TODO: create operation to 'add' counters?
public interface Counter<K> extends Map<K, Integer>
{
    public Integer increaseCount(K key);
}
