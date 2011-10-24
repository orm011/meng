/**
 * 
 */
package com.twitter.dataservice.sharding;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.LinkedBlockingQueue;

public class SlidingWindowCycleIterator<K> implements Iterator<List<K>>{
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