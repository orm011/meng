package com.twitter.dataservice.sharding;

import com.twitter.dataservice.shardutils.Edge;
import com.twitter.dataservice.shardutils.Node;
import com.twitter.dataservice.shardutils.Shard;
import com.twitter.dataservice.shardutils.Vertex;

import java.util.*;

//this maps edges/vertices to nodes (to distinguish
// it from a sharding, which maps edges/vertices to shards.
//those can then be mapped to nodes.  don't use this.
public class ShardLib implements IShardLib
{
    ISharding sharding;
    IShardPrimitives state;

    //my approaches would be implemented as
    //different 'ISharding'
    public ShardLib(ISharding keyToShardStrategy, IShardPrimitives zkState){
        sharding = keyToShardStrategy;
        state = zkState;
    }

    public Node getNode(Edge e){
        Shard relevantShard = sharding.getShardForEdgeQuery(e);
        Set<Node> reps = state.getReplicaSet(relevantShard);
        return getNodeForQuery(reps);
    }
    
    public Collection<Node> getNodes(Vertex v){
        Collection<Shard> involved= sharding.getShardForVertexQuery(v);
        List<Set<Node>> optionList = new LinkedList<Set<Node>>();

        for (Shard sh: involved) {
          optionList.add(state.getReplicaSet(sh));
        }

        return getNodesForQuery(optionList);
    }


  // here you can place logic for things like considering
  // mastership and live nodes
    private List<Node> getNodesForQuery(List<Set<Node>> options){
        Iterator<Set<Node>> it = options.iterator();
        List<Node> answer = new LinkedList<Node>();

        assert it.hasNext();//non-empty
        while (it.hasNext()){
          answer.add(getNodeForQuery(it.next()));
        }

        return answer;
    }

    private Node getNodeForQuery(Set<Node> options){
        Iterator<Node> nodeit = options.iterator();
        assert nodeit.hasNext();
        Node answer = nodeit.next();
        return answer;
    }

}