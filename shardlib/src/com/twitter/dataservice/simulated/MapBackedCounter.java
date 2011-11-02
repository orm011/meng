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

    @Override
    public void clear()
    {
        internalMap.clear();
    }

    @Override
    public boolean containsKey(Object key)
    {
        return internalMap.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value)
    {
        return internalMap.containsValue(value);
    }

    @Override
    public Set<java.util.Map.Entry<K, Integer>> entrySet()
    {
        return internalMap.entrySet();
    }

    @Override
    public Integer get(Object key)
    {
        if (internalMap.containsKey(key)){
            return internalMap.get(key);
        } else {
            return 0;
        }
    }

    @Override
    public boolean isEmpty()
    {
        return internalMap.isEmpty();
    }

    @Override
    public Set<K> keySet()
    {
        return internalMap.keySet();
    }

    @Override
    public Integer put(K key, Integer value)
    {
        return internalMap.put(key, value);
    }

    @Override
    public void putAll(Map<? extends K, ? extends Integer> m)
    {
        internalMap.putAll(m);
    }

    @Override
    public Integer remove(Object key)
    {
        return internalMap.remove(key);
    }

    @Override
    public int size()
    {
        return internalMap.size();
    }

    @Override
    public Collection<Integer> values()
    {
        return internalMap.values();
    }

}
