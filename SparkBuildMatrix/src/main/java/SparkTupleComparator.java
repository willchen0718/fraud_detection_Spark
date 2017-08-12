package udel.weiyang.spark;

import scala.Tuple2;
import java.util.Comparator;
import java.io.Serializable;

public class SparkTupleComparator
implements Comparator<Tuple2<Long, String>>, Serializable {
    
    static final SparkTupleComparator Comparator = new SparkTupleComparator();
    
    public int compare(Tuple2<Long, String> t1, Tuple2<Long, String> t2){
        return t1._1.compareTo(t2._1);
    }
}