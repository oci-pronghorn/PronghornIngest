package com.ociweb.pronghorn.components.ingestion.dynamic.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RunningStdDev {
    final static Logger logger = LoggerFactory.getLogger(RunningStdDev.class);
   
    private long sampleCount;
    private double oldMean;
    private double newMean;
    private double oldStd;
    private double newStd;
    private double min;
    private double max;
    
    public void clear() {
        synchronized (this) {
            sampleCount = 0;
            oldMean = 0;
            newMean = 0;
            oldStd = 0;
            newStd = 0;
            min = Long.MAX_VALUE;
            max = Long.MIN_VALUE;
        }
    }
    
    public RunningStdDev(boolean checkDistribution) {
        clear();
    }
    
    private static double probabilityDensityY(double x, double variance, double stdDev, double mean) {
        //computed http://www.had2know.com/academics/normal-distribution-probability-calculator.html
        double pow = -((x-mean)*(x-mean))/(2d*variance);
        double numerator = Math.pow(Math.E,pow);
        double denominator = stdDev*Math.sqrt(2d*Math.PI);
        double result = numerator/denominator;
        return result;
    }

    public static void sample(RunningStdDev runStdDev, double sample) {
            if (Double.isNaN(sample)) {
                return; //do not add sample if its not a number
            }

            if (sample<runStdDev.min) {
            	runStdDev.min = sample;
            }
            if (sample>runStdDev.max) {
            	runStdDev.max = sample;
            }
            runStdDev.sampleCount++;
            // See Knuth TAOCP vol 2, 3rd edition, page 232
            if (runStdDev.sampleCount == 1)
            {
            	runStdDev.oldMean = runStdDev.newMean = sample;
            	runStdDev.oldStd = 0.0;
            } else {
            	runStdDev.newMean = runStdDev.oldMean + (sample - runStdDev.oldMean)/runStdDev.sampleCount;
            	runStdDev.newStd = runStdDev.oldStd + (sample - runStdDev.oldMean)*(sample - runStdDev.newMean);
        
                // set up for next iteration
            	runStdDev.oldMean = runStdDev.newMean; 
            	runStdDev.oldStd = runStdDev.newStd;
            }

    }
    

    public static String toString(RunningStdDev runStdDev) {
        return "count:"+sampleCount(runStdDev)+" mean:"+mean(runStdDev)+" std:"+stdDeviation(runStdDev)+" max:"+maxSample(runStdDev)+" min:"+minSample(runStdDev);
    }
    
    public static double mean(RunningStdDev runStdDev) {
        return runStdDev.newMean;
    }
    
    public static double maxSample(RunningStdDev runStdDev) {
        return runStdDev.max;
    }
    
    public static double minSample(RunningStdDev runStdDev) {
        return runStdDev.min;
    }
    
    public static double variance(RunningStdDev runStdDev) {
        return ( (runStdDev.sampleCount > 1) ? runStdDev.newStd/(runStdDev.sampleCount - 1) : 0.0 );
    }
    
    public static double stdDeviation(RunningStdDev runStdDev) {
        return Math.sqrt(variance(runStdDev));
    }

    public static long sampleCount(RunningStdDev runStdDev) {
        return runStdDev.sampleCount;
    }
    
    
    
    public static double probabilityDensity(RunningStdDev runStdDev, double x) {
        //inputs
        double variance = variance(runStdDev);
        double stdDev = Math.sqrt(variance);
        double mean = mean(runStdDev);
        
        return probabilityDensityY(x, variance, stdDev, mean);
        
    }

 
    
    
}
