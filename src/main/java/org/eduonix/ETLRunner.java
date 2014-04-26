package org.eduonix;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.ExecType;
import org.eduonix.etl.PigETL;
import org.eduonix.etl.SeismicETL;

import java.io.IOException;
import java.util.Map;

/**
 * http://www.qubole.com/pig-function-cheat-sheet/
 */
public class ETLRunner {

    private static String uniquePathId = ""+System.currentTimeMillis();

    public static void main(String[] args) throws Exception {

        Path testDataInput = new Path("./testData","seismic1");
        Path extractDataTestOutput = new Path("./EtlDataOut/"+uniquePathId+"/extract");

        PigETL pigResult = new PigETL(
                testDataInput.toString(),
                extractDataTestOutput.toString(), ExecType.LOCAL);

    }
}
