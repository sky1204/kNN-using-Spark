package edu.uwb.css534;
import java.io.Serializable;


/**
 *  Object to hold intermediate results.
 */
class OutputArgs implements Serializable {

    double distance;
    int isSkin;

    public OutputArgs(double distance, int isSkin) {
        this.distance = distance;
        this.isSkin = isSkin;
    }
}
