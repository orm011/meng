/**
 * 
 */
package com.twitter.dataservice.sharding;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

//TODO: eventually replace this with google collections implementation
public class CycleIterator<K> implements Iterator<K>{
      
      private Iterable<K> col;
      private Iterator<K> it = null;
      private boolean wrapped = false;
      
      public CycleIterator(Set<K> col){
          if (col == null || col.size() == 0)
              throw new IllegalArgumentException("no null or empty collections accepted");
          this.col = col;
          it = col.iterator();
      }
      
      //assumes the iteration order of the set is consistent with the given iterator,
      //and consistent for the given set every time it is called
      public CycleIterator(Iterable<K> col, Iterator<K> startingPosition){
          this.col = col;
          it = startingPosition;
      }
      
      //it always has next unless empty.
      @Override
      public boolean hasNext(){
          return !(col == null || !col.iterator().hasNext());
      }
      
      @Override
      public K next() throws NoSuchElementException {
          if (!this.hasNext()){
                  throw new NoSuchElementException();              
          }

          if (wrapped = !it.hasNext())
              it = col.iterator();
              
          K toReturn = it.next();
          
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