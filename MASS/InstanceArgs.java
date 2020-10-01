package edu.uwb.css534;
import java.io.Serializable;

/**
 *  Input args for the places.
 */
class InstanceArgs implements Serializable {

    String fileName;
    RGB rgb;
    int placesSize;
    int splitBy;

    public InstanceArgs(String fileName, RGB rgb, int placesSize, int splitBy) {
       this.fileName = fileName;
       this.rgb  = rgb;
       this.placesSize = placesSize;
       this.splitBy = splitBy;
    }
}
