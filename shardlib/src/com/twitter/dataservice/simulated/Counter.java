package com.twitter.dataservice.simulated;

import java.util.Map;


//TODO: create operation to 'add' counters?
public interface Counter<K>
{
    public Integer increaseCount(K key);
    
    public Integer getCount(K key);
    
    public int getTotal();
}
