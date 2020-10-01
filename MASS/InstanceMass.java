package edu.uwb.css534;

import edu.uw.bothell.css.dsl.MASS.MASS;
import edu.uw.bothell.css.dsl.MASS.Place;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

public class InstanceMass extends Place {

    // function identifiers
    public static final int init_ = 0;
    public static final int calculate_ = 1;

    private static String fileName = null;
    private FileInputStream file = null;

    private List<RGB> mydata = new ArrayList<>();

    private InstanceArgs instanceArgs;

    public InstanceMass() {
        super();
    }

    public InstanceMass(Object arg) {

    }

    public Object callMethod( int functionId, Object argument ) {
        switch( functionId ) {
            case init_:      return init( argument );
            case calculate_: return calculateDistance(argument);
        }
        return null;
    }


    /**
     *
     * Initialization function for reading the files and normalizing the RGB values.
     *
     * */
    private Object init( Object arg )  {
        instanceArgs = ( ( InstanceArgs )arg );
        String line;

        fileName = instanceArgs.fileName;

        // Read a given data set as a whole
        synchronized( fileName ) {
            if ( file == null ) {
                // open the file
                try {
                    file = new FileInputStream( fileName );
                    FileReader reader = new FileReader(fileName);
                    BufferedReader traningFileBuffReader = new BufferedReader(reader);

                    while((line= traningFileBuffReader.readLine()) != null) {
                        String[] parts = line.split(" ");
                        RGB rgb = new RGB(Double.parseDouble(parts[0]), Double.parseDouble(parts[1]),  Double.parseDouble(parts[2]), Integer.parseInt(parts[3]));
                        mydata.add(rgb);
                    }

                } catch( Exception e ) {
                    e.printStackTrace( );
                };
            }
        }

        //find min and max
        double minR = mydata.get(0).getR();
        double maxR = mydata.get(0).getR();

        double minG = mydata.get(0).getG();
        double maxG = mydata.get(0).getG();

        double minB = mydata.get(0).getB();
        double maxB = mydata.get(0).getB();

        for (int i = 0; i < mydata.size(); i++) {
            if (mydata.get(i).getR() > maxR) {
                maxR = mydata.get(i).getR();
            } else if (mydata.get(i).getR() < minR) {
                minR = mydata.get(i).getR();
            }

            if (mydata.get(i).getG() > maxG) {
                maxG = mydata.get(i).getG();
            } else if (mydata.get(i).getG() < minG) {
                minG = mydata.get(i).getG();
            }

            if (mydata.get(i).getB() > maxB) {
                maxB = mydata.get(i).getB();
            } else if (mydata.get(i).getB() < minB) {
                minB = mydata.get(i).getB();
            }
        }


        //standardization
        for (int i = 0; i < mydata.size(); i++) {
            double curr = mydata.get(i).getR();
            double res = (curr - minR) / (maxR - minR);
            mydata.get(i).setR(res);

            curr = mydata.get(i).getG();
            res = (curr - minG) / (maxG - minG);
            mydata.get(i).setG(res);

            curr = mydata.get(i).getB();
            res = (curr - minB) / (maxB - minB);
            mydata.get(i).setB(res);

        }

        RGB otherRgb = instanceArgs.rgb;

        // Normalization the input.
        double r = (otherRgb.getR() - minR) / (maxR - minR);
        double g = (otherRgb.getG() - minG) / (maxG - minG);
        double b = (otherRgb.getB() - minB) / (maxB - minB);

        otherRgb.setB(b);
        otherRgb.setG(g);
        otherRgb.setR(r);

        instanceArgs.rgb = otherRgb;

        return null;
    }

    private OutputArgsWrapper calculateDistance(Object arg) {

        RGB otherRgb = instanceArgs.rgb;
        List<OutputArgs> outputArgsList = new ArrayList<>();
	    int startIndex = this.getIndex()[0]*instanceArgs.splitBy;
	    int endIndex = this.getIndex()[0]*instanceArgs.splitBy + instanceArgs.splitBy;

        if(this.getIndex()[0] == (instanceArgs.placesSize/instanceArgs.splitBy) - 1) {
            endIndex += instanceArgs.placesSize - endIndex;
        }
	
        MASS.getLogger().debug("Start index " + startIndex + " EndIndex " + endIndex);

        for(int i=startIndex; i < endIndex; i++) {
            RGB rgb = mydata.get(i);
	    
	        OutputArgs outputArgs = new OutputArgs(Math.sqrt((rgb.getR() - otherRgb.getR()) * (rgb.getR() - otherRgb.getR()) + (rgb.getG() - otherRgb.getG()) *
                    (rgb.getG() - otherRgb.getG()) +
                    (rgb.getB() - otherRgb.getB()) * (rgb.getB() - otherRgb.getB())), rgb.skin());

            outputArgsList.add(outputArgs);
        }

        return new OutputArgsWrapper(outputArgsList);
    }
}
