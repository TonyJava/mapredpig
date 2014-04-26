package org.eduonix.udf;

import com.google.common.collect.Maps;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;

import java.io.IOException;
import java.math.BigDecimal;
import java.text.ParseException;
import java.util.Map;

/**
 * http://stackoverflow.com/questions/20093629/converting-tuple-of-bags-to-multiple-tuples-in-pig-using-a-java-udf
 */
public class PigAggregatorUDF extends EvalFunc<String> {


    @Override
    public String exec(Tuple input) throws IOException {

        try {
            // Verify the input is valid, logging to a Hadoop counter if not.
            if (input == null || input.size() < 1) {
                throw new IOException("Not enough arguments to " + this.getClass().getName() + ": got " + input.size() + ", expected at least 1");
            }

            if (input.get(0) == null) {
                System.out.println("found null ");
                return null;
            }

           String tmp = input.toDelimitedString(",");

            return parseStringToMap(tmp);
        } catch (ExecException e) {
            System.out.println("Error in " + getClass() + " with input " + input+e.getLocalizedMessage());
            throw new IOException(e);
        }
    }

    protected String parseStringToMap(String line) {

        String[] tmpArr = line.split(",");

        BigDecimal value = BigDecimal.valueOf(Double.valueOf(tmpArr[0]));
        BigDecimal value1 = BigDecimal.valueOf(Double.valueOf(tmpArr[1]));
        BigDecimal value2 = BigDecimal.valueOf(Double.valueOf(tmpArr[2]));

        value = value.movePointRight(6);
        value1 = value.movePointRight(6);
        value2 = value.movePointRight(6);
        String values = value+" : "+value1+" : "+value2;

        return values;

        }



    }


