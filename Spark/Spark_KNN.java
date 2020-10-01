
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import javax.xml.transform.Source;
import scala.Tuple2;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import java.util.Arrays;
import java.util.Iterator;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import scala.*;
import java.util.*;
import org.apache.spark.api.java.function.Function;
    

public class KNN{ 
        
    public static void main(String[] args)
{
    // start Sparks and read a given input file
    String inputFile = args[0];
    SparkConf conf = new SparkConf( ).setAppName( "KNN K nearest neighbors" );
    JavaSparkContext jsc = new JavaSparkContext( conf );
    JavaRDD<String> lines = jsc.textFile( inputFile );

    //takes in user input for the values of test data
    int K=Integer.parseInt(args[1]);
    final Integer Rcord=Integer.parseInt(args[2]);
    final Integer Gcord=Integer.parseInt(args[3]);
    final Integer Bcord=Integer.parseInt(args[4]);

    // now start a timer
    long startTime = System.currentTimeMillis();   

    //mapping input values into RDD, Data class is a separate class to store attributes
    JavaPairRDD< java.lang.Double, Data> network = lines.mapToPair(line -> {
                                String[] splitdata=line.split(" ");                                                        

                                Data data=new Data();         
                                //converting the coordinated into final variables as non-final variables cannot be used in lambda functions
                                data.R=Integer.parseInt(splitdata[0]);
                                final Integer a=Integer.parseInt(splitdata[0]);
                                data.G=Integer.parseInt(splitdata[1]);
                                final Integer b=Integer.parseInt(splitdata[1]);
                                data.B=Integer.parseInt(splitdata[2]);
                                final Integer c=Integer.parseInt(splitdata[2]);
                                data.category1=Integer.parseInt(splitdata[3]);
                                //calculating distance from the training data to the test value                                                                                     
                                java.lang.Double distance=Math.sqrt((a-Rcord)*(a-Rcord)+(b-Gcord)*(b-Gcord)+(c-Bcord)*(c-Bcord));
                                //returing tuple values
                                return new Tuple2<>(distance, data); 
                            }                                           
                            );

    //sorting the RDD based on the distance values
    JavaPairRDD<java.lang.Double,Data> customSorted= network.sortByKey();

    //variables for storing count values for white and black skintone
    int countWhite=0;
    int countBlack=0;
    int i=0;
    int finalCategory;
    String skinTone;

    //storing the customSotedRDD into a map
    java.util.Map<java.lang.Double,Data> networkMap=customSorted.collectAsMap();

    //voting, calculating the number of K neighbors belonging to white and black skintone
    Iterator<java.util.Map.Entry<java.lang.Double, Data>> iterator =  networkMap.entrySet().iterator();
    int removed = 0;
    while(iterator.hasNext() && removed <K) {
        iterator.next();
        java.util.Map.Entry<java.lang.Double, Data> entry = iterator.next(); 
        Data dataKNeighbor=entry.getValue();
        if(dataKNeighbor.category1==1)
        countWhite++;

        else
        countBlack++;
        removed++;
    }

    if(countWhite>countBlack)
    {finalCategory=1;
     skinTone="White";
    }

    else
    {finalCategory=2; 
     skinTone="Black";
    }   
    
    //printing out the final category
    System.err.println("Out of "+K+" neighbors "+countWhite+" belonged to skintone white and "+countBlack+" belonged to Black skintone");
    System.err.println("The final category is "+finalCategory+" and the skin color is "+skinTone);

    //Calculating end time
    long endTime = System.currentTimeMillis();
    long totalTime=(endTime-startTime);
    System.err.println("Total time is: " + totalTime+ " microseconds");
}
}