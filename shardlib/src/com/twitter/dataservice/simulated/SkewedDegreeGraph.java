/**
 * generates data and workload for a skewed degree graph
 */
package com.twitter.dataservice.simulated;


import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Random;
 
import org.apache.commons.math.MathException;
import org.apache.commons.math.distribution.ZipfDistributionImpl;
import org.apache.commons.lang.NotImplementedException;
import com.twitter.dataservice.shardutils.Edge;
import com.twitter.dataservice.shardutils.Pair;
import com.twitter.dataservice.shardutils.Vertex;
import com.twitter.dataservice.parameters.GraphParameters;
import com.twitter.dataservice.parameters.WorkloadParameters;

//graph generation class, using the zipf distribution idea, and not really faithfully generating the graph, but only the degrees
public class SkewedDegreeGraph implements Graph {
      
      private final int maxDegree;
      private final int numVertices;
      private final int[] degreeTable;      
      
      //because we cannot control it perfectly
      private final int numEdges;
      private final int disconnected;
      /*
       * @param degreeSkewParameter must be > 0, the larger it is the less common it is
       * to have any nodes with large degree.
       * @param graphSize: number of vertices in the graph
       */
      
      static public SkewedDegreeGraph makeSkewedDegreeGraph(GraphParameters gp){
          return SkewedDegreeGraph.makeSkewedDegreeGraph(gp.getNumberVertices(), gp.getNumberEdges(), 
                  gp.getUpperDegreeBound(), gp.getDegreeSkewParameter());
      }
      
      static public SkewedDegreeGraph makeSkewedDegreeGraph(int numVertices, int targetNumberOfEdges, int upperDegreeBound, double degreeSkew){
          if (numVertices < 1 || upperDegreeBound< 1 || degreeSkew <= 0)
              throw new IllegalArgumentException();

          int[] aDegreeTable  = new int[numVertices];
          ZipfDistributionImpl degGenerator = new ZipfDistributionImpl(upperDegreeBound, degreeSkew);
              
          int count = 0;
          for (int currentVertex = 0; currentVertex < numVertices; currentVertex++){
              
              try {
                  aDegreeTable[currentVertex] = degGenerator.sample();
                  count += aDegreeTable[currentVertex];
              } catch (MathException me) {
                  throw new RuntimeException(me);
              }
          }
          
          float normalizationFactor = ((float) targetNumberOfEdges )/count;
          
          for (int currentVertex = 0; currentVertex < numVertices; currentVertex++){
              int maybeDegree = Math.round((aDegreeTable[currentVertex]*normalizationFactor));
              aDegreeTable[currentVertex] =  maybeDegree + (maybeDegree == 0 ? 1:0);
          }
          
          return new SkewedDegreeGraph(aDegreeTable);
      }
      
      //constructor for testing. //will mutate the degree table
      SkewedDegreeGraph(int[] degreeTable){
          Arrays.sort(degreeTable); //helps doing a lot of other computations

          if (degreeTable.length < 1) throw new IllegalArgumentException();
          if (degreeTable[0] <= 0) throw new IllegalArgumentException();
          numVertices = degreeTable.length;
          maxDegree = degreeTable[degreeTable.length - 1];
          this.degreeTable = degreeTable;
          
          int accumulatedDegree = 0;
          int accumulatedDisconnected = 0;
          for (int j = 0; j < degreeTable.length; j++) {
              accumulatedDegree += degreeTable[j];
              accumulatedDisconnected += (degreeTable[j] == 0? 1:0);
          }
          
          numEdges = accumulatedDegree;
          disconnected = accumulatedDisconnected;
      }
      
      public int getActualDegree(){
          return this.numEdges;
      }
      
      public int getActualMaxDegree(){
          return this.maxDegree;
      }
      /*
       * used for plotting (degree distribution)
       */
      public int getDegree(int index){
          return degreeTable[index];
      }
      
      public int getDisconnnectedVertices(){
          return disconnected;
      }

      
      public class FanoutIterator implements Iterator<Pair<Integer, int[]>>{ 
    	  
        int currentVertex = disconnected;
        @Override
        public boolean hasNext()
        {
            return currentVertex < degreeTable.length;
        }

        @Override
        public Pair<Integer, int[]> next()
        {
            int[] fanouts = new int[degreeTable[currentVertex]];
            for (int i = 0; i < fanouts.length; i++){
                fanouts[i] = i;
            }
            int old = currentVertex++;
            return new Pair<Integer, int[]>(old, fanouts);
        }

        @Override
        public void remove()
        {
            throw new NotImplementedException();
        }
          
      }
      
      public class SkewedDegreeGraphIterator implements Iterator<Edge>{

        int currentVertex = disconnected - 1;
        int remainingEdges = 0;
          
        @Override
        public boolean hasNext()
        {
            return (remainingEdges > 0 || (currentVertex + 1) < degreeTable.length);
        }

        @Override
        public Edge next()
        {
            if (!hasNext()) throw new NoSuchElementException();
            if (0 == remainingEdges) {
                currentVertex++;
                remainingEdges = degreeTable[currentVertex];
            }
            
            return new Edge(new Vertex(currentVertex), new Vertex(--remainingEdges));
        }

        @Override
        public void remove()
        {
            throw new UnsupportedOperationException();            
        }
      };
            
      //use: to query at benchmark time
      public class QueryWorkloadIterator implements Iterator<Query>{
        
        private final WorkloadParameters parameters;
        final ZipfDistributionImpl sampler; 
        final Random internalRandomness = new Random();
        
        int queriesSoFar = 0;
          //this can choose a skew but not necessarily a correlation with degree skew
        public QueryWorkloadIterator(WorkloadParameters params){
            sampler = new ZipfDistributionImpl(numVertices, params.getQuerySkew());
            parameters = params;
        }
          
        @Override
        public boolean hasNext()
        {
            return queriesSoFar < parameters.getNumberOfQueries();
        }

        @Override
        public Query next()
        {
            int index;
            
            try
            {
                index = sampler.sample() - 1;
            } catch (MathException e)
            {
                throw new RuntimeException(e);
            }
            
            ++queriesSoFar;
            int maxVertex = degreeTable[index];
            
            if (internalRandomness.nextFloat() < parameters.getPercentEdgeQueries()/100.0){
                return Query.edgeQuery(new Vertex(index), 
                        new Vertex(internalRandomness.nextInt(maxVertex)));
            } else {
                return Query.fanoutQuery(new Vertex(index));
            }
            
        }
            

        @Override
        public void remove()
        {
            throw new UnsupportedOperationException();
        }
          
      }

    @Override
    public Iterator<Query> workloadIterator(WorkloadParameters params)
    {
        return new QueryWorkloadIterator(params);
    }

    @Override
    public Iterator<Edge> graphIterator()
    {
        return new SkewedDegreeGraphIterator();
    }

    @Override
    public Iterator<Pair<Integer, int[]>> fanoutIterator()
    {
        return new FanoutIterator();
    }
    
}