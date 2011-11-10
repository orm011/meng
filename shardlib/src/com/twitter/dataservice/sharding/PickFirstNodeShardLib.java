package com.twitter.dataservice.sharding;

import com.twitter.dataservice.shardutils.Edge;
import com.twitter.dataservice.shardutils.Node;
import com.twitter.dataservice.shardutils.Pair;
import com.twitter.dataservice.shardutils.Shard;
import com.twitter.dataservice.shardutils.Vertex;

import java.util.*;

//this maps edges/vertices to nodes (to distinguish
// it from a sharding, which maps edges/vertices to shards.
//those can then be mapped to nodes.  don't use this.
public class PickFirstNodeShardLib implements INodeSelectionStrategy
{
    ISharding sharding;
 //   IShardPrimitives state;

    //my approaches would be implemented as
    //different 'ISharding'
    public PickFirstNodeShardLib(ISharding keyToShardStrategy, IShardPrimitives zkState){
        sharding = keyToShardStrategy;
        //state = zkState;
    }

    public Node getNode(Edge e){
        Pair<Shard, Collection<Node>> relevantShard = sharding.getShardForEdgeQuery(e);
        return getNodeForQuery(relevantShard.getRight());
    }
    
    public Collection<Node> getNodes(Vertex v){
        List<Pair<Shard, Collection<Node>>> involved= sharding.getShardForVertexQuery(v);
        List<Collection<Node>> optionList = new LinkedList<Collection<Node>>();

        for (Pair<Shard, Collection<Node>> p: involved) {
          optionList.add(p.getRight());
        }

        return getNodesForQuery(optionList);
    }

    


  // here you can place logic for things like considering
  // mastership and live nodes
    private List<Node> getNodesForQuery(List<Collection<Node>> options){
        Iterator<Collection<Node>> it = options.iterator();
        List<Node> answer = new LinkedList<Node>();

        assert it.hasNext();//non-empty
        while (it.hasNext()){
          answer.add(getNodeForQuery(it.next()));
        }

        return answer;
    }

    private Node getNodeForQuery(Collection<Node> options){
        Iterator<Node> nodeit = options.iterator();
        assert nodeit.hasNext();
        Node answer = nodeit.next();
        return answer;
    }

}