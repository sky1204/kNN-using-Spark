package edu.uwb.css534;

import java.util.*;
import java.io.*;
import edu.uw.bothell.css.dsl.MASS.*;
import edu.uw.bothell.css.dsl.MASS.logging.LogLevel;


public class MassKnn {

    public static void  main(String[] args) throws Exception {

        MASS.init();
        MASS.setLoggingLevel( LogLevel.DEBUG );

        //taking user input for the data point
        double inputR = Double.parseDouble(args[0]);
        double inputG = Double.parseDouble(args[1]);
        double inputB = Double.parseDouble(args[2]);

        RGB testInput =  new RGB(inputR, inputG, inputB, 0);

        int placeSize = Integer.parseInt(args[3]);
        int splitBy = Integer.parseInt(args[4]);

        InstanceArgs instanceArgs = new InstanceArgs("training.txt", testInput, placeSize, splitBy);

        MASS.getLogger().debug("Initializing the places");

        Places instances = new Places( 1, InstanceMass.class.getName(), null, placeSize/splitBy);
        MASS.getLogger().debug("Starting the program with instance call");
        long startTime = System.currentTimeMillis();

        instances.callAll(InstanceMass.init_, instanceArgs);

        MASS.getLogger().debug("Completed instance init");

        // dummy objects
        Object[] inputObjects = new Object[placeSize];

        // Need to handle the last remaining elements, as size may not be divisible by number of nodes.
        Object[] objects =  instances.callAll(InstanceMass.calculate_, inputObjects);

        int k = (int) Math.sqrt(placeSize);

        Map<Double,Integer> distanceToClass = new HashMap<>();
        //using treeset to sort the distances.
        Set<Double> distances = new TreeSet<>();

        for (int i = 0; i < objects.length; i++) {
            OutputArgsWrapper outputArgsWrapper = (OutputArgsWrapper) objects[i];

            for(OutputArgs outputArgs : outputArgsWrapper.outputArgsList) {

                distances.add(outputArgs.distance);
                distanceToClass.put(outputArgs.distance, (int) outputArgs.isSkin);
            }
        }

        int firstClassCounter = 0;
        int secondClassCounter = 0;

        Iterator<Double> itr = distances.iterator();

        // Checking the nearest points to find whether "1" is near or "2" is near.
        while (firstClassCounter != k && secondClassCounter != k) {
            if (distanceToClass.get(itr.next()).intValue() == 1) {
                firstClassCounter++;
                System.out.println("class model 1");
            } else if (distanceToClass.get(itr.next()).intValue() == 2) {
                secondClassCounter++;
            }
        }

        if (firstClassCounter == k) {
            System.out.println("Class for test instance object is: 1 and the skin color is white");
        } else if (secondClassCounter == k) {
            System.out.println("Class for test instance object is: 2 and the skin color is black");
        }

        long stopTime = System.currentTimeMillis();
        System.err.println("Execution time in milli secs " + (stopTime - startTime));

        MASS.finish();
    }


}



