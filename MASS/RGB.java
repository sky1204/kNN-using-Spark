package edu.uwb.css534;

import java.io.Serializable;

/**
 *  Model file for storing RGB values.
 */
class RGB implements Serializable {

    private double R, G, B;
    int isSkin;

    public RGB(double R, double G, double B, int isSkin) {
        this.R = R;
        this.G = G;
        this.B = B;
        this.isSkin = isSkin;
    }

    void setR(double R) {
        this.R = R;
    }

    void setG(double G) {
        this.G = G;
    }

    void setB(double B) {
        this.B = B;
    }

    double getR() {
        return R;
    }

    double getG() {
        return G;
    }

    double getB() {
        return B;
    }

    int skin() {
        return isSkin;
    }
}
