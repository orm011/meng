package com.twitter.dataservice.simulated;


import java.awt.Container;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import javax.swing.JFrame;
import javax.swing.JPanel;

import org.apache.commons.math.distribution.ZipfDistributionImpl;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.junit.Test;

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
}