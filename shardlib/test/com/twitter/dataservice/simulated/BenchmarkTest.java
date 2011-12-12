package com.twitter.dataservice.simulated;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import org.apache.log4j.FileAppender;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.junit.Test;

import com.twitter.dataservice.parameters.GraphParameters;
import com.twitter.dataservice.parameters.WorkloadParameters;


public class BenchmarkTest
{    

    //only testing the logger api.
    @Test public void foo(){
        PropertyConfigurator.configure("/Users/oscarm/workspace/oscarmeng/shardlib/log4j.properties");
        Logger lg = Logger.getLogger(BenchmarkTest.class);
        try
        {
            lg.removeAllAppenders();
            Date now = new Date(System.currentTimeMillis());
            String folder = new SimpleDateFormat("yyyy-MM-dd").format(now);
            String name = "saved_on_" + new SimpleDateFormat("yyyy-MM-dd'T'HH.mm.ss.SSS").format(now);
            lg.addAppender(new FileAppender(new org.apache.log4j.PatternLayout("%r [%t] %p %c{2} %x %d %m%n"), 
                    "/Users/oscarm/workspace/oscarmeng/shardlib/logs/archive/"+ name + ".log"));
        } catch (IOException e)
        {
            e.printStackTrace();
        }
        
        lg.debug("PROBANDO");
    }
    
    // checking java's own property format
    @Test public void propertyTest(){
        Properties prop = new Properties();
        prop.setProperty(GraphParameters.DEGREE_RATIO_BOUND, "200");
        prop.setProperty(GraphParameters.AVERAGE_DEGREE, "100");
        prop.setProperty(GraphParameters.SKEW_PARAMETER, "1.0");
        prop.setProperty(GraphParameters.NUMBER_VERTICES, "200");
        
        prop.setProperty(WorkloadParameters.NUMBER_OF_QUERIES, "1000");
        prop.setProperty(WorkloadParameters.PERCENT_EDGE_QUERIES, "0");
        prop.setProperty(WorkloadParameters.PERCENT_FANOUT_QUERIES, "100");
        prop.setProperty(WorkloadParameters.QUERY_SKEW, "0.01");
        
        try
        {
            prop.store(new FileOutputStream(new File("testConfigFile.properties")), "hello");
        } catch (FileNotFoundException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    
    
    @Test public void readPropertyTest(){
        Properties p = new Properties();
        
        try
        {
            p.load(new FileReader("testPropertyfile.properties"));
        } catch (FileNotFoundException e)
        {
            e.printStackTrace();
        } catch (IOException e)     
        {
            e.printStackTrace();
        }
    }
    
}