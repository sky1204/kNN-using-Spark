# KNN Using Parallel Programming
Faster execution for KNN (compared to the sequential code)is achieved using parallel programming paradigms: Spark, MPI and MapReduce. 
## KNN Introduction

K-nearest neighbors (kNN) algorithm is a simple, easy-to-implement supervised machine learning algorithm that can be used to solve both classification and regression problems
Given an unclassified point, we can assign it to a group by observing what group its nearest neighbors belong to. The final step is calculating the K nearest neighbors and voting. The class which most of the K neighbors belong to is assigned to the test data.
### DataSet 
The dataset that is are using has four attributes, the first three being the color ratio of red, green and blue color respectively. The last value represents the skin color(category/class) based on the RGB (red, green, blue) ratio either the skin color is white or black, represented by integer values 0 or 1.
### Performance Improvement
MPI: 1.47
MapReduce: 1.3 
Spark: 2.55
