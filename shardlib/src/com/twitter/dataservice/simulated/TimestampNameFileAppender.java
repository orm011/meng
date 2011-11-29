package com.twitter.dataservice.simulated;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
 
import org.apache.log4j.FileAppender;
import org.apache.log4j.Layout;
import org.apache.log4j.spi.ErrorCode;

public class TimestampNameFileAppender extends FileAppender
{
    
    static String logName;
    static String baseName;
    static String parent;
    

    /**
    * make a new file for every run
    * original source ripped from http://veerasundar.com
    */
    public TimestampNameFileAppender() {;
    }
     
    public TimestampNameFileAppender(Layout layout, String filename,
            boolean append, boolean bufferedIO, int bufferSize)
            throws IOException {
        super(layout, filename, append, bufferedIO, bufferSize);
    }
     
    public TimestampNameFileAppender(Layout layout, String filename,
            boolean append) throws IOException {
        super(layout, filename, append);
    }
     
    public TimestampNameFileAppender(Layout layout, String filename)
            throws IOException {
        super(layout, filename);
    }
     
    public void activateOptions() {
    if (fileName != null) {
        try {
            fileName = getNewLogFileName();
            setFile(fileName, fileAppend, bufferedIO, bufferSize);
        } catch (Exception e) {
            errorHandler.error("Error while activating log options", e,
                    ErrorCode.FILE_OPEN_FAILURE);
        }
    }
    }
     
    private String getNewLogFileName() {

        if (logName == null){
            if (fileName != null) {
                Date now = new Date(System.currentTimeMillis());
                String folder = new SimpleDateFormat("yyyy-MM-dd").format(now);
                String timeStamp = new SimpleDateFormat("yyyy-MM-dd'T'HH.mm.ss.SSS").format(now);

                final String DOT = ".";
                final String HIPHEN = "-";
                final File logFile = new File(fileName);
                final String fileName = logFile.getName();
                String newFileName = "";

                final int dotIndex = fileName.indexOf(DOT);
                if (dotIndex != -1) {
                    // the file name has an extension. so, insert the time stamp
                    // between the file name and the extension
                    newFileName = fileName.substring(0, dotIndex) + HIPHEN
                    + timeStamp + DOT
                    + fileName.substring(dotIndex + 1);
                } else {
                    // the file name has no extension. So, just append the timestamp
                    // at the end.
                    newFileName = fileName + HIPHEN + timeStamp;
                }

                logName = logFile.getParent() + File.separator +  folder + File.separator + newFileName;
                baseName = folder + File.separator + newFileName;
            } else {  
            //TODO: this will be constantly redone unless we change the value to something else    
                logName = null;
            }
        }

        return logName;
    }
    
    public static String getLogName(){
        return baseName;
    }

}
