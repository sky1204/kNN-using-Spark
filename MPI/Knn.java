import java.util.*;
import java.io.*;
import mpi.*;


class Instance {
    private double R,G,B,isSkin;


    //Initializing R,G,B and skintone 
    public Instance(double R, double G, double B, int isSkin){
        this.R = R;
        this.G = G;
        this.B = B;
        this.isSkin = isSkin;
    }

    void setR(double R){
        this.R = R;
    }

    void setG(double G){
        this.G = G;
    }

    void setB(double B){
        this.B = B;
    }
    double getR(){
        return R;
    }

    double getG(){
        return G;
    }

    double getB(){
        return B;
    }

    double skin(){
        return isSkin;
    }


    //calculating the distances
    double calculateDistance(double otherR, double otherG, double otherB){
        return Math.sqrt((R - otherR) * (R - otherR) + (G - otherG) * (G - otherG) + (B - otherB) * (B - otherB));

    }

}

public class Knn {

    public static void  main(String[] args) throws Exception {

        //Initializing the MPI constructs
        MPI.Init(args);
        int my_rank = MPI.COMM_WORLD.Rank();
        int size = MPI.COMM_WORLD.Size();

        String line;
        FileReader reader = new FileReader("training.txt");
        BufferedReader traningFileBuffReader = new BufferedReader(reader);

        List<Instance> instances=new ArrayList<>();

        while((line= traningFileBuffReader.readLine()) != null) {
            String[] parts = line.split(" ");
            Instance instance = new Instance(Double.parseDouble(parts[0]), Double.parseDouble(parts[1]),
         Double.parseDouble(parts[2]), Integer.parseInt(parts[3]));
	    instances.add(instance);
        }

        traningFileBuffReader.close();
        reader.close();

        FileReader inputReader = new FileReader("input.txt");
        BufferedReader inputBufferedReader = new BufferedReader(inputReader);

        Optional<String> inputString = inputBufferedReader.lines().findFirst();

        if(!inputString.isPresent()) {
            throw new RuntimeException("Input string cannot be null");
        }

        int length=instances.size();
        int stripe=length/size;

        long startTime = System.currentTimeMillis();

        //find min and max
        double minR = instances.get(0).getR();
        double maxR = instances.get(0).getR();

        double minG = instances.get(0).getG();
        double maxG = instances.get(0).getG();

        double minB = instances.get(0).getB();
        double maxB = instances.get(0).getB();

        for (int i = 0; i < instances.size(); i++) {
            if (instances.get(i).getR() > maxR) {
                maxR = instances.get(i).getR();
            } else if (instances.get(i).getR() < minR) {
                minR = instances.get(i).getR();
            }

            if (instances.get(i).getG() > maxG) {
                maxG = instances.get(i).getG();
            } else if (instances.get(i).getG() < minG) {
                minG = instances.get(i).getG();
            }

            if (instances.get(i).getB() > maxB) {
                maxB = instances.get(i).getB();
            } else if (instances.get(i).getB() < minB) {
                minB = instances.get(i).getB();
            }
        }


        //standardization
        for (int i = 0; i < instances.size(); i++) {
            double curr = instances.get(i).getR();
            double res = (curr - minR) / (maxR - minR);
            instances.get(i).setR(res);

            curr = instances.get(i).getG();
            res = (curr - minG) / (maxG - minG);
            instances.get(i).setG(res);

            curr = instances.get(i).getB();
            res = (curr - minB) / (maxB - minB);
            instances.get(i).setB(res);

        }

        int k = (int)Math.sqrt(instances.size());

       


        double[] distances = new double[instances.size()];
        int[] classes = new int[instances.size()];

        String[] inputSplit = inputString.get().split(" ");


        double r = Double.parseDouble(inputSplit[0]);
        double g = Double.parseDouble(inputSplit[1]);
        double b = Double.parseDouble(inputSplit[2]);
        r = (r - minR) / (maxR - minR);
        g = (g - minG) / (maxG - minG);
        b = (b - minB) / (maxB - minB);

        //Calculate distances to all the points in the stripe from the given point

        for (int i = stripe*my_rank; i < stripe*(my_rank+1); i++) {
            double d = instances.get(i).calculateDistance(r, g, b);
            distances[i] = d;
            classes[i]= ((int)instances.get(i).skin());
        }

        

        //Master receives the distances from all the slaves
        if(my_rank==0) {
            for (int rank=1; rank < size; rank++) {
                MPI.COMM_WORLD.Recv(distances, stripe*rank, stripe, MPI.DOUBLE, rank, 0);
                MPI.COMM_WORLD.Recv(classes, stripe*rank, stripe, MPI.INT, rank, 0);
            }

        } else {
            MPI.COMM_WORLD.Send(distances, stripe*my_rank, stripe, MPI.DOUBLE, 0, 0);
            MPI.COMM_WORLD.Send(classes, stripe*my_rank, stripe, MPI.INT, 0, 0);
        }

	if(my_rank == 0) {

	    //put distances along with the original skin tone in the Map
        Set<Double> distancesSet = new TreeSet<>();
        Map<Double, Integer> distanceToClass = new HashMap<>();


        for(int i=0; i < instances.size(); i++) {
            distancesSet.add(distances[i]);
            distanceToClass.put(distances[i], classes[i]);
        }

        int firstClassCounter = 0;
        int secondClassCounter = 0;

        System.out.println("Distance size " + distancesSet.size());

        Iterator<Double> itr = distancesSet.iterator();

        //Iterate the set to get the maximum vote for the category
        while (firstClassCounter != k && secondClassCounter != k) {
            if (distanceToClass.get(itr.next()).intValue() == 1) {
                firstClassCounter++;
            } else if (distanceToClass.get(itr.next()).intValue() == 2) {
                secondClassCounter++;
            }
        }

        if (firstClassCounter == k) {
            System.out.println("The Final category is 1 and the skin colour is white");
        } else if (secondClassCounter == k) {
            System.out.println("The Final category is 2 and the skin colour is Black ");
        }

        long stopTime = System.currentTimeMillis();
        System.err.println("Execution time in milli secs " + (stopTime - startTime));
    }

      MPI.Finalize();     
   }
}

