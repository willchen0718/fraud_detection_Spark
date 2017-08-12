package udel.weiyang.spark;

import scala.Tuple2;
import java.util.Comparator;
import java.io.Serializable;
import java.util.Map;
import java.util.HashMap;

public class InitialMatrix {
    
    Map<String, Integer> label_to_pos;
    double[][] transMatrix;
    int numStates;
    
    public InitialMatrix(String typeState){
        String[] states = typeState.split(",");
        numStates = states.length;
        
        transMatrix = new double[numStates][numStates];
        label_to_pos = new HashMap<String, Integer>();
        
        int pos = 0;
        for (String state : states) {
            label_to_pos.put(state, pos);
            pos++;
        }
    }
    
    public void addTo(String present, String future, int val) {
        
        int row = label_to_pos.get(present);
        int col = label_to_pos.get(future);
        
        transMatrix[row][col] += val;
    }
    
    public void normalizeRows() {
        // laplace smoothing function
        for (int r = 0; r < numStates; r++) {
            
            boolean gotZeroCount = false;
            for (int c = 0; c < numStates && !gotZeroCount; c++) {
                gotZeroCount = transMatrix[r][c] == 0;
            }
            
            if (gotZeroCount) {
                for (int c = 0; c < numStates; c++) {
                    transMatrix[r][c] += 1;
                }
            }
        }
        
        //normalize
        double rowSum = 0;
        for (int r = 0; r < numStates; r++) {
            double check_sum = 0.0;
            rowSum = getRowSum(r);
            for (int c = 0; c < numStates; c++) {
                transMatrix[r][c] = transMatrix[r][c] / rowSum;
                check_sum += transMatrix[r][c];
            }
        }
    } 
    
    public int getRowSum(int row) {
        int sum = 0;
        for (int c = 0; c < numStates; c++) {
            sum += transMatrix[row][c];
        }
        return sum;
    }    
}
