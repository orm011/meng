package com.twitter.dataservice.sharding;

import java.io.IOException;

import com.twitter.dataservice.shardingpolicy.*;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
//TODO: add hash function used by vertex hashing here so that pig can precompute partitions and we can load separately
public class VERTEXHASHSHARD extends EvalFunc<Integer>
{
    
    /*
     * input: left id, right id
     */
    public Integer exec(Tuple input) throws IOException {
        if (input == null){
            return null;
        }
        
        try {
            Integer left = (Integer) input.get(0);
            Integer right = (Integer) input.get(1);
            Integer numNodes = (Integer) input.get(2);
            Integer numShards = (Integer) input.get(3);
            
            return VertexHashSharding.hash(left, right, numNodes, numShards);
 
        } catch (Exception e){
            throw new IOException("Caught exception processing input row ", e);
        }
    }
}