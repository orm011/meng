package com.twitter.dataservice.sharding;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.LinkedBlockingQueue;

import com.twitter.dataservice.shardutils.Edge;
import com.twitter.dataservice.shardutils.IHashFunction;
import com.twitter.dataservice.shardutils.Node;
import com.twitter.dataservice.shardutils.Pair;
import com.twitter.dataservice.shardutils.Shard;
import com.twitter.dataservice.shardutils.Token;
import com.twitter.dataservice.shardutils.Vertex;

//most basic sharding. edge goes to  a Token.
public class TwoTierHashSharding implements ISharding
{

  SortedMap<Token,Node> shards = new TreeMap<Token, Node>();
  Map<Vertex,Integer> exceptions; //figure out how to populate this and if we need to.

  final static HierarchicalHashFunction hashfun = new TwoTierHashSharding.HierarchicalHashFunction();

  IShardPrimitives state; //not sure about this reference.
  
  
  //Methods to keep state up to date across all shardlibs. (todo later)
  //sets up the local state based the given zkState (initialization)
  public TwoTierHashSharding(IShardPrimitives zkState){
       //gets snapshot and sends it
       //need to go through each the state and populate the sorted map
       //shards = new TreeMap<Token,Node>();     
  }
  
  //for test purposes TODO: remove this/make private, somehow
  public TwoTierHashSharding(List<Pair<Token, Node>> shardState){
      for (Pair<Token, Node> pair : shardState){
          shards.put(pair.getLeft(), pair.getRight());
      }
  }
  
  public void applyStateUpdate(IShardUpdate zkUpdate){
      //applies local state based on a delivered update
  }
  
  //client inits the remote zkState (do we do this any differently from a state update?)
  public void initRemoteHashSharding(){
      //takes snapshot of local state and sends it
  }
  
  public void publishStateUpdate(){
      //sends update to zk (assumes you have applied it/waits for it to come back?/applies it here?)
  }
  
  //Methods to read the shard state  
  @Override public Pair<Shard,Collection<Node>> getShardForEdgeQuery(Edge e) {
        Token tok = hashfun.hash(e);
        assert !shards.isEmpty();
  
        // search within shardlist for an individual shard where it fits.
        // deal with wrap-around
        CycleIterator<Map.Entry<Token,Node>> it = new CycleIterator<Map.Entry<Token,Node>>(shards.entrySet(), 
                shards.tailMap(tok).entrySet().iterator());
    
        CycleIterator<Map.Entry<Token,Node>> ot = new CycleIterator<Map.Entry<Token,Node>>(shards.entrySet(), 
                shards.tailMap(tok).entrySet().iterator());
    
        SlidingWindowCycleIterator<Map.Entry<Token,Node>> window = new SlidingWindowCycleIterator<Map.Entry<Token,Node>>(ot, 
                TwoTierHashSharding.REPLICATION_FACTOR);
        
        List<Node> nodes = new LinkedList<Node>();
        assert it.hasNext();
        
        Token shardtok = it.next().getKey();
        List<Map.Entry<Token, Node>> successors = window.next();
        for (Map.Entry<Token, Node> mape: successors){
            nodes.add(mape.getValue());
        }
        
        return new Pair<Shard, Collection<Node>>(new Shard(shardtok),  nodes);
  }
  
  public List<Pair<Shard,Collection<Node>>> getShardForVertexQuery(Vertex v) {
      List<Pair<Shard,Collection<Node>>> answer = new LinkedList<Pair<Shard,Collection<Node>>>();
      Pair<Token,Token> ends = hashfun.hash(v);
      SortedMap<Token, Node> tail = shards.tailMap(ends.getLeft());
 
      CycleIterator<Map.Entry<Token,Node>> it = new CycleIterator<Map.Entry<Token,Node>>(shards.entrySet(), 
              tail.entrySet().iterator());
  
      CycleIterator<Map.Entry<Token,Node>> ot = new CycleIterator<Map.Entry<Token,Node>>(shards.entrySet(), 
              tail.entrySet().iterator());
      
      SlidingWindowCycleIterator<Map.Entry<Token,Node>> window = new SlidingWindowCycleIterator<Map.Entry<Token,Node>>(ot, 
              TwoTierHashSharding.REPLICATION_FACTOR);
  
      {
          Map.Entry<Token, Node> ent = null;
          //iterator deals with wrap
          while(it.hasNext() && !it.wrappedAround() && (ent = it.next()).getKey().compareTo(ends.getRight()) <= 0){
              LinkedList<Node> nodes = new LinkedList<Node>();
              
              List<Map.Entry<Token, Node>> entries = window.next();
              for (Map.Entry<Token, Node> mape: entries){
                  nodes.add(mape.getValue());
              }
              
              answer.add(new Pair<Shard, Collection<Node>>(new Shard(ent.getKey()), nodes));
          }
      }
            
      return answer;
  }

  //gets a view of the shard tokens within the range given as input together with the respective
  //replicas implied by the replication factor
  public List<Pair<Token, Collection<Node>>> hashRingRangeQuery(Pair<Token, Token> tokenRange){
      List<Pair<Token,Collection<Node>>> answer = new LinkedList<Pair<Token,Collection<Node>>>();
      SortedMap<Token, Node> tail = shards.tailMap(tokenRange.getLeft());
 
      CycleIterator<Map.Entry<Token,Node>> it = new CycleIterator<Map.Entry<Token,Node>>(shards.entrySet(), 
              tail.entrySet().iterator());
  
      CycleIterator<Map.Entry<Token,Node>> ot = new CycleIterator<Map.Entry<Token,Node>>(shards.entrySet(), 
              tail.entrySet().iterator());
      
      SlidingWindowCycleIterator<Map.Entry<Token,Node>> window = new SlidingWindowCycleIterator<Map.Entry<Token,Node>>(ot, 
              TwoTierHashSharding.REPLICATION_FACTOR);
  
      {
          Map.Entry<Token, Node> ent = null;
//          //iterator deals with wrap
//          System.out.println("hasNext?" + it.hasNext());
//          System.out.println("wrappedAround?" + it.wrappedAround());
//          
//          System.out.println("it.next().getKey()" +  (ent = it.next()).getKey());
//          System.out.println("tokenRange.getRight()" + tokenRange.getRight());
//          System.out.println("ent.compareTo(blah blah)" + ent.getKey().compareTo(tokenRange.getRight()));
//          System.out.println("wrappedAround after?" + it.wrappedAround());
//          System.out.println("it.next().getKey()" + (ent = it.next()d).getKey());
//          System.out.println("wrappedAround?" + it.wrappedAround());
          
          
          if (!it.hasNext()) {
              throw new IllegalArgumentException("should have at least one shard");
          }
          
          do {
           ent = it.next();
           LinkedList<Node> nodes = new LinkedList<Node>();
           
           List<Map.Entry<Token, Node>> entries = window.next();
           for (Map.Entry<Token, Node> mape: entries){
               nodes.add(mape.getValue());
           }
           
           answer.add(new Pair<Token, Collection<Node>>(ent.getKey(), nodes));
          } while(ent.getKey().compareTo(tokenRange.getRight()) <= 0 && !it.wrappedAround());          
      }
      
      return answer;
  }
  
  //TODO: clean up by making a new tree class? and putting stuff there?
  //iterates through elements normally but wraps around in the same order when 
  // elements end. 
  public static class CycleIterator<K> implements Iterator<K>{
      
      private Set<K> col;
      private Iterator<K> it = null;
      private boolean wrapped = false;
      
      public CycleIterator(Set<K> col){
          this.col = col;
      }
      
      //assumes the iteration order of the set is consistent with the given iterator,
      //and consistent for the given set every time it is called
      public CycleIterator(Set<K> col, Iterator<K> startingPosition){
          this.col = col;
          it = startingPosition;
      }
      
      //it always has next unless empty.
      @Override
      public boolean hasNext(){
          return col == null | col.size() > 0;
      }
      
      @Override
      public K next() throws NoSuchElementException {
          if (!this.hasNext()){
              throw new NoSuchElementException();
          } else if (it == null || !it.hasNext()){
              it = col.iterator();
          }
                    
          K toReturn = it.next();
          wrapped = !it.hasNext();          
          return toReturn;
      }
      
      public boolean wrappedAround(){
          return this.wrapped;
      }

    @Override
    public void remove()
    {
        it.remove();
    }
            
  }
  
  //takes a CycleIterator, but at each step returns a window
  public static class SlidingWindowCycleIterator<K> implements Iterator<List<K>>{
  //Methods to write to the Shard state.
      LinkedBlockingQueue<K> internalQueue;
      CycleIterator<K> internalIterator;
      int wSize;
      
      //window must have size > 0
      public SlidingWindowCycleIterator(CycleIterator<K> ci, int windowSize){
          if (windowSize <= 0)
              throw new IllegalArgumentException("windowSize must be greater than 0");
          
          internalQueue = new LinkedBlockingQueue<K>(windowSize);
          internalIterator = ci;
          wSize = windowSize;
          
          //initialize current window. 
          if (internalIterator.hasNext()){
              for(int i = 0; i < wSize; i++)
                  internalQueue.add(internalIterator.next());
          }
                    
      }
      
      //assumes internal iterator is cyclic 
      @Override
      public boolean hasNext(){
          return internalIterator != null && internalIterator.hasNext();
      }
      
      //returns a list of elements in the order they appeared
      @Override
      public List<K> next() throws NoSuchElementException {
          if (!hasNext()){
              throw new NoSuchElementException();
          }
          

          List<K> answer = new LinkedList<K>(internalQueue);
         
          //update internal state. it should not be empty
          internalQueue.remove();
          internalQueue.add(internalIterator.next());

          return answer;
      }

    @Override
    public void remove()
    {
        internalIterator.remove();
    }
  }
  
  @Override public Collection<Edge> getEdgesWithinShard(Shard sh, Collection<Edge> edges) {
        //to be executed at data nodes.
      // for this ISharding method, it would work by looking at token ranges.
      return null;
  }

 //TODO: define thresholds with some basis
  private final static int PROMOTE_NODE = 200000;
  private final static int TOLERANCE = 2000;
  private final static int DEMOTE_NODE = PROMOTE_NODE - TOLERANCE;
  private final static int REPLICATION_FACTOR = 3;
 
  //also temporary
  private final static int SHARDS_PER_SPECIAL_NODE = 4;
  //private final static int OPTIMAL_SINGLE_NODE_SHARD_SIZE = PROMOTE_NODE;
  
  void maybeUpdateVertexCategory(Vertex v, Integer count){
    //look at the exceptions map and at the count and see if we should reevalute this vertex's category
     //TODO: implement more complex policy based on number of shards desirable
    
      //this part needs to behave as if done atomically, and the local indexed versions of the sharding state need to 
      //be consistent with the actual state in zK
    
     if (exceptions.containsKey(v) && count < DEMOTE_NODE){
         demoteVertex(v);
     } else if (!exceptions.containsKey(v) && count > PROMOTE_NODE){
         promoteVertex(v);
     }     
    
     
  }

  private void demoteVertex(Vertex v){
  }
  
  private List<Token> partitionRange(Vertex v){
      return null;
  }
  
  private void promoteVertex(Vertex v){
      
     //local state changes
    
    //List<Pair<Token, Node>> newShards = hashfun.hashPartition(, v); 
    
    //TODO: this should not be a simple add, but an add
    //to a some kind of sorted Data structure.
    //now set the map from shards
//    Set<Node> lowNodes = new HashSet<Node>();
//    Set<Node> highNodes = new HashSet<Node>();
//    state.setReplicaSet(low, lowNodes);
//    state.setReplicaSet(high, highNodes);
  }

  public static class HierarchicalHashFunction implements IHashFunction {
  //by hierarchical we mean there are two levels to it (one per vertex)
  //basically the token for (edge(v1,v2)) is  (hash(v1),hash(v2))
  //this way, all edges for a given node are in a contiguous token range,
  //and I can use the same representation for all kinds of shards

   private static MessageDigest hashfun;

   static {
     try {
      hashfun = MessageDigest.getInstance("MD5");
     } catch (NoSuchAlgorithmException e) {
       throw new RuntimeException(e);
     }
   }

  public Token hash(Edge e) {
    byte[] left = hashfun.digest(e.getLeftEndpoint().toByteArray());
    byte[] right = hashfun.digest(e.getRightEndpoint().toByteArray());
    byte[] hashed = new byte[left.length + right.length];
    System.arraycopy(left, 0, hashed, 0, left.length);
    System.arraycopy(right, 0, hashed, left.length, right.length);

    return new Token(hashed);
  }

  public Pair<Token, Token> hash(Vertex v) {
    byte[] leftside = hashfun.digest(v.toByteArray());

    byte[] upperend = Arrays.copyOf(leftside, leftside.length*2);  
    Arrays.fill(upperend, leftside.length, upperend.length, (byte)0xff);
    
    byte[] lowerend = Arrays.copyOf (leftside, leftside.length*2);
    Arrays.fill(lowerend, leftside.length, lowerend.length, (byte)0);
 
    
    return new Pair<Token,Token>(new Token(lowerend), new Token(upperend));
  }

  @Override
  public Token hash(Node n)
  {
      byte[] left = hashfun.digest(n.toByteArray());
      byte[] ans = Arrays.copyOf(left, 2*left.length);
      Arrays.fill(ans, left.length, ans.length, (byte)0xff);
      return new Token(ans);
  }

  @Override
  public List<Pair<Token, Node>> hashPartition(List<Node> nodes, Vertex v)
  {
      byte[] left = hashfun.digest(v.toByteArray());
      
      List<Pair<Token,Node>> results = new LinkedList<Pair<Token,Node>>();
      for (Node n: nodes){
          byte[] ringPos = Arrays.copyOf(left, 2*left.length);
          byte[] hash = hashfun.digest(n.toByteArray());
          System.arraycopy(hash, 0, ringPos, left.length, ringPos.length);          
          
          results.add(new Pair<Token, Node>(new Token(ringPos), n));
      }
      
      
      
      return results;      
  }
  
  
}
  
}