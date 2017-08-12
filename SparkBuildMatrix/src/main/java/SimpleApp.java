package udel.weiyang.spark;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.SparkConf;
import scala.Tuple2;

import java.util.*;
import java.io.*;

public class SimpleApp{
    
    public static void main(String[] args) {
        
        final String states = "LNL,MNL,HNL,"
        + "LHL,MHL,HHL,"
        + "LNN,MNN,HNN,"
        + "LHN,MHN,HHN,"
        + "LNS,MNS,HNS,"
        + "LHS,MHS,HHS";
        
        String sortFile = "hdfs://master:9000/user/hadoop/input/usa.txt";
        SparkConf conf = new SparkConf().setAppName("Simple Application");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile(sortFile);
        
// Read file from hdfs: JavaPairRDD<CustomerID, Tuple2<TransationID, TransationType>>
        JavaPairRDD<String, Tuple2<Long, String>> pairs = lines.mapToPair(new PairFunction<String, String, Tuple2<Long, String>>(){
            public Tuple2<String, Tuple2<Long, String>> call(String s){
                String[] tokens = s.split(",");
                Tuple2<Long, String> timeValue = new Tuple2<Long, String>(Long.parseLong(tokens[1].trim()),tokens[2].trim());
                return new Tuple2<String, Tuple2<Long, String>> (tokens[0].trim(), timeValue);
            }
        });

// Group RDDs by CustomerID
        JavaPairRDD<String, Iterable<Tuple2<Long, String>>> groups = pairs.groupByKey(40);

// Sort RDDs by TransationID    
        JavaPairRDD<String, Iterable<Tuple2<Long, String>>> sorted = groups.mapValues(new Function<Iterable<Tuple2<Long, String>>,Iterable<Tuple2<Long, String>>>(){
            public Iterable<Tuple2<Long, String>> call(Iterable<Tuple2<Long, String>> s){
                List<Tuple2<Long, String>> list = new ArrayList<Tuple2<Long, String>>();
                for (Tuple2<Long, String> item: s){
                    list.add(item);
                }
                Collections.sort(list, SparkTupleComparator.Comparator);
                return list;
            }
        });

// Build matrix for each customer
        JavaPairRDD<String, double[][]> rawMatrix = sorted.mapValues(new Function<Iterable<Tuple2<Long, String>>, double[][]>(){
            public double[][] call(Iterable<Tuple2<Long, String>> s){
                
                Iterator<Tuple2<Long, String>> it = s.iterator();
                InitialMatrix matrix = new InitialMatrix(states);
                
                String currentState = it.next()._2;
                while (it.hasNext()){
                    String nextState = it.next()._2;
                    matrix.addTo(currentState, nextState, 1);
                    currentState = nextState;
                }
                matrix.normalizeRows();
                return matrix.transMatrix;
            }
        });

// Convert matrix to string
	JavaPairRDD<String, String> cookedMatrix = rawMatrix.mapValues(new Function<double[][], String>(){
	    public String call(double[][] s){
		    String delimiter = System.getProperty("line.separator");
     		    StringJoiner sb = new StringJoiner(delimiter);
		    for (double[] row:s){
   			 sb.add(Arrays.toString(row));
		    }
		    return sb.toString();
	    }
	});
// Save file to hdfs
	cookedMatrix.saveAsTextFile("hdfs://master:9000/user/hadoop/Spark_output");

// Stop Spark job	
	sc.stop();
    }
}



