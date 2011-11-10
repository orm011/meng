package com.twitter.dataservice.plotting;

import java.awt.Font;

import com.jrefinery.chart.Axis;
import com.jrefinery.chart.AxisNotCompatibleException;
import com.jrefinery.chart.HorizontalNumberAxis;
import com.jrefinery.chart.JFreeChart;
import com.jrefinery.chart.Plot;
import com.jrefinery.chart.VerticalNumberAxis;
import com.jrefinery.chart.XYPlot;

//plot class
public class Histogram
{
    public static JFreeChart createHistogram(String xLabel, String title, HistogramDataSource data){
        final String YLABEL = "freq";
        Axis xAxis = new HorizontalNumberAxis(xLabel);
        Axis yAxis = new VerticalNumberAxis(YLABEL);
        
        try {
            Plot xyPlot = new XYPlot(null, xAxis, yAxis);
            return new JFreeChart(title, new Font("Arial", Font.BOLD, 24), data, xyPlot);
        } catch (AxisNotCompatibleException e){
            throw new RuntimeException(e);
        }
    }
    
    public static void plotHistogram(String xLabel, String title, HistogramDataSource data){
        JFreeChart hist = createHistogram(xLabel, title, data);
        //TODO. the rest of it.
    }
    
}
