import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import scala.Tuple2;

/**                                                                                                                        
 * Vertex Attributes
 */
public class Data implements Serializable {
    
    Integer category1;   //the value of class representing skin color, 1 for black, 0 for white
    Integer R;          //the value of the ratio of red color
    Integer G;          //the value of the ratio of green color
    Integer B;          //the value of the ratio of blue color               

    public Data(){
    //empty constructor
    }


    public Data( Integer dist, Integer R, Integer G,Integer B, Integer category1 ){
       
        this.R=R;
        this.G=G;
        this.B=B;
        this.category1 = category1;
    }
}
