package edu.uwb.css534;

import java.io.Serializable;
import java.util.List;

/**
 *  Wrapper object holding intermediate distance calculation results.
 */
public class OutputArgsWrapper implements Serializable {

    List<OutputArgs> outputArgsList;

    public OutputArgsWrapper(List<OutputArgs> outputArgsList) {
        this.outputArgsList = outputArgsList;
    }
}
