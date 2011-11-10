/**
 * generates data and workload for a skewed degree graph
 */
package com.twitter.dataservice.simulated;

import java.awt.Container;
import java.awt.Font;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;

import javax.swing.JFrame;
import javax.swing.JPanel;

import org.apache.commons.math.MathException;
import org.apache.commons.math.distribution.ZipfDistributionImpl;

import com.jrefinery.chart.Axis;
import com.jrefinery.chart.AxisNotCompatibleException;
import com.jrefinery.chart.DefaultCategoryDataSource;
import com.jrefinery.chart.HorizontalNumberAxis;
import com.jrefinery.chart.JFreeChart;
import com.jrefinery.chart.JFreeChartPanel;
import com.jrefinery.chart.Plot;
import com.jrefinery.chart.VerticalNumberAxis;
import com.jrefinery.chart.XYDataSource;
import com.jrefinery.chart.XYPlot;
import com.jrefinery.util.ui.Swing;
import com.twitter.dataservice.shardutils.Edge;
import com.twitter.dataservice.shardutils.Vertex;
import com.twitter.dataservice.simulated.BenchmarkData.Query;
import com.twitter.dataservice.simulated.BenchmarkData.WorkloadParams;

//graph generation class, using the zipf distribution idea, and not really faithfully generating the graph, but only the degrees
public class SkewedDegreeGraph implements BenchmarkData {
      
      private ZipfDistributionImpl degGenerator;
      private int maxDegree;
      private int graphSize;
      private ArrayList<Integer> vertexDegrees;

      /*
       * @param degreeSkewParameter must be > 0, the larger it is the less common it is
       * to have any nodes with large degree.
       * @param graphSize: number of vertices in the graph
       */
      public SkewedDegreeGraph(int graphSize, int maxDegree, double degreeSkewParamater){
          if (graphSize < 1 || maxDegree < 1 || degreeSkewParamater <= 0)
              throw new IllegalArgumentException();
          this.graphSize = graphSize;
          this.maxDegree = maxDegree;
              vertexDegrees = new ArrayList<Integer>(graphSize);
          degGenerator = new ZipfDistributionImpl(maxDegree, degreeSkewParamater);
                    
          for (int currentVertex = 0; currentVertex < graphSize; currentVertex++){
              try {
                  vertexDegrees.add(currentVertex, degGenerator.sample());
              } catch (MathException me) {
                  throw new RuntimeException(me);
              }
          }
      }
      
      public void plotGraphSkew(int seconds){
          Integer[][] degreeFrequency = new Integer[maxDegree][];
          for (int i = 0; i < maxDegree; i++){
              degreeFrequency[i] = new Integer[]{0};
          }
          
          for (int i = 0; i < graphSize; i++){
              degreeFrequency[vertexDegrees.get(i) - 1][0]++;
          }
          
          
//          public static JFreeChart createXYChart(XYDataSource data) {
//
//              Axis xAxis = new HorizontalNumberAxis("X");
//              Axis yAxis = new VerticalNumberAxis("Y");
//
//              try {
//                Plot xyPlot = new XYPlot(null, xAxis, yAxis);
//                return new JFreeChart("XY Plot", new Font("Arial", Font.BOLD, 24), data, xyPlot);
//              }
//              catch (AxisNotCompatibleException e) {  // work on this later...
//                return null;
//              }
//            }


          
          class GraphSkewChart extends JFrame {       
              public GraphSkewChart(Number[][] data){
                  JFreeChart  chart = JFreeChart.createVerticalBarChart(new DefaultCategoryDataSource(data));
                  Container contentPane = new JPanel();
                  
                  contentPane.add(new JFreeChartPanel(chart));
                  setContentPane(contentPane);
              }
          };
                    
          GraphSkewChart ch = new GraphSkewChart(degreeFrequency);
          ch.pack();
          Swing.centerFrameOnScreen(ch);
          ch.show();
          try { Thread.sleep(seconds*1000); }
          catch (InterruptedException ie){ System.out.println("plot closed"); }
      } 
            
      //use: to place it into the system
      public class SkewedDegreeGraphIterator implements Iterator<Edge>{

        int currentVertex = -1;
        int currentEdge = 0;
          
        @Override
        public boolean hasNext()
        {
            return (currentEdge > 0 || currentVertex < graphSize - 1);
        }

        @Override
        public Edge next()
        {            
            if (!hasNext()) throw new NoSuchElementException();

            if (0 == currentEdge) {
                ++currentVertex;
                currentEdge = vertexDegrees.get(currentVertex) - 1;
            } else {
                --currentEdge;
            }
            
            return new Edge(new Vertex(currentVertex), new Vertex(currentEdge));
        }

        @Override
        public void remove()
        {
            throw new UnsupportedOperationException();            
        }          
      };
            
      //use: to query at benchmark time
      public class QueryWorkloadIterator implements Iterator<Query>{
        
        private final WorkloadParams parameters;
        final ZipfDistributionImpl sampler; 
        final Random internalRandomness = new Random();
        
        int queriesSoFar = 0;
          //this can choose a skew but not necessarily a correlation with degree skew
        public QueryWorkloadIterator(WorkloadParams params){
            sampler = new ZipfDistributionImpl(graphSize, params.getSkew());
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
            //TODO: right now only edge queries, check that later
            int index;
            
            try
            {
                index = sampler.sample();
            } catch (MathException e)
            {
                throw new RuntimeException(e);
            }
            
            ++queriesSoFar;
            int maxVertex = vertexDegrees.get(index - 1);
            
            return Query.edgeQuery(new Vertex(index), 
                    new Vertex(internalRandomness.nextInt(maxVertex)));
        }

        @Override
        public void remove()
        {
            throw new UnsupportedOperationException();
        }
          
      }

    @Override
    public Iterator<Query> workloadIterator(WorkloadParams params)
    {
        return new QueryWorkloadIterator(params);
    }

    @Override
    public Iterator<Edge> graphIterator()
    {
        return new SkewedDegreeGraphIterator();
    }
  }
      