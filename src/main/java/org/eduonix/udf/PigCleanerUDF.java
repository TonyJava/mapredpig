package org.eduonix.udf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.pig.*;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import java.io.IOException;
import org.apache.hadoop.fs.Path;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


/**
 * Created by ubu on 4/20/14.
 */
public class PigCleanerUDF extends LoadFunc  {

    private static final String DELIM = "    ";
    private static final int DEFAULT_LIMIT = 226;
    private int limit = DEFAULT_LIMIT;
    private RecordReader reader;

    private TupleFactory tupleFactory;


    public PigCleanerUDF() {

        tupleFactory = TupleFactory.getInstance();
    }


    @Override
    public InputFormat getInputFormat() throws IOException {
        return new TextInputFormat();

    }

    /**
     * the input in this case is a TSV, so split it, make sure that the requested indexes are valid,
     */
    @Override
    public Tuple getNext() throws IOException {
        Tuple tuple = null;
        List values = new ArrayList();

        try {
            boolean notDone = reader.nextKeyValue();
            if (!notDone) {
                return null;
            }
            Text value = (Text) reader.getCurrentValue();

            if(value != null) {
                String parts[] = value.toString().split(DELIM);

                    for(int k = 0 ; k <  parts.length;k++) {
                        values.add(parts[k]);
                    }

                }

                tuple = tupleFactory.newTuple(values);


        } catch (InterruptedException e) {
            // add more information to the runtime exception condition.
            int errCode = 6018;
            String errMsg = "Error while reading input";
            throw new ExecException(errMsg, errCode,
                    PigException.REMOTE_ENVIRONMENT, e);
        }

        return tuple;

    }

    @Override
    public void prepareToRead(RecordReader reader, PigSplit pigSplit)
            throws IOException {
        this.reader = reader; // note that for this Loader, we don't care about the PigSplit.
    }

    @Override
    public void setLocation(String location, Job job) throws IOException {
        FileInputFormat.setInputPaths(job, location); // the location is assumed to be comma separated paths.

    }


}
