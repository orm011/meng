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
import com.twitter.dataservice.parameters.SamplableBuilder;
import com.twitter.dataservice.parameters.WorkloadParameters;
import com.twitter.dataservice.parameters.SamplableBuilder.DistributionType;


//graph generation class, not really faithfully generating the graph, but only the degrees 
//(used for the fanout only)
public class SkewedDegreeGraph implements Graph {
      
      private final int maxDegree;
      private final int numVertices;
      private final int[] degreeTable;      
      private final int numEdges;
      private final int disconnected;
      
      public int getNumVertices() {
		return numVertices;
	}


      /*
       * @param degreeSkewParameter must be > 0, the larger it is the less common it is
       * to have any nodes with large degree.
       * @param graphSize: number of vertices in the graph
       * this kept here for compatibility with previous api.
       */
      static public SkewedDegreeGraph makeSkewedDegreeGraph(int numVertices, int targetNumberOfEdges, int upperDegreeBound, double degreeSkew){
    	  GraphParameters.Builder gpb = new GraphParameters.Builder();
    	  SamplableBuilder rand = new SamplableBuilder();
    	  rand.setHighLimit(upperDegreeBound + 1);
    	  rand.setLowLimit(1);
    	  rand.setSkew((float)degreeSkew);
    	  rand.setType(DistributionType.ZIPF);
    	  rand.setZipfbins(upperDegreeBound);
    	  
    	  GraphParameters gp = gpb.numberVertices(numVertices).degreeGenerator(rand.build()).build();
    	  return makeSkewedDegreeGraph(gp);
      }
      
      static public SkewedDegreeGraph makeSkewedDegreeGraph(GraphParameters gp){
          return SkewedDegreeGraph.makeSkewedDegreeGraph(gp.getNumberVertices(), gp.getSamplable());
      }
      
      static public SkewedDegreeGraph makeSkewedDegreeGraph(int numVertices, Samplable gen){
          if (numVertices < 1 )
              throw new IllegalArgumentException();

          int[] aDegreeTable  = new int[numVertices];
          int count = 0;
          for (int currentVertex = 0; currentVertex < numVertices; currentVertex++){
                  aDegreeTable[currentVertex] = gen.sample();
                  if (!(aDegreeTable[currentVertex] > 0)) throw new AssertionError(); //por si las moscas
                  count += aDegreeTable[currentVertex];
          }
          
          return new SkewedDegreeGraph(aDegreeTable);
      }
      
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
        final Samplable sampler; 
        final Random internalRandomness = new Random();
        
        int queriesSoFar = 0;
          //this can choose a skew but not necessarily a correlation with degree skew
        public QueryWorkloadIterator(WorkloadParameters params){
        	sampler = params.getSamplable();
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
            int index = sampler.sample();
            
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