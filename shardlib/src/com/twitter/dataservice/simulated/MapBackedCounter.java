package com.twitter.dataservice.simulated;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class MapBackedCounter<K> implements Counter<K>
{
    HashMap<K, Integer> internalMap;
    public MapBackedCounter(int initialCapacity){
        internalMap = new HashMap<K, Integer>(initialCapacity);
    }
    
    public MapBackedCounter(){
        internalMap = new HashMap<K, Integer>();
    }
    
    @Override
    public Integer increaseCount(K key)
    {
        Integer ans;
        
        if (internalMap.containsKey(key)){
            ans = internalMap.put(key, internalMap.get(key) + 1);
        } else {
            internalMap.put(key, 1);
            ans = 0; 
        }
        
        return ans;
    }

    public void clear()
    {
        internalMap.clear();
    }

    public Set<Map.Entry<K, Integer>> entrySet()
    {
        return internalMap.entrySet();
    }

    public Integer getCount(K key)
    {
        if (internalMap.containsKey(key)) return internalMap.get(key);
        else return 0;
    }

    public boolean isEmpty()
    {
        return internalMap.isEmpty();
    }

    public int size()
    {
        return internalMap.size();
    }
}
