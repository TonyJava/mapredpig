package org.eduonix.etl;


import com.google.common.collect.Maps;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.data.Tuple;


import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * This class operates by ETL'ing the dataset into pig, and then 
 * implements the "statistics" contract in the functions which follow. 
 * 
 * The pigServer is persisted through the life of the class, so that the intermediate
 * data sets created in the constructor can be reused.
 */
public class PigETL {

    PigServer pigServer ;


    private static String jatPath ="/home/ubu/mapredpig/pigmodule.jar";



    public PigETL(String inputPath, String outputPath, ExecType ex) throws Exception{

        System.out.println("inputPath "+inputPath);

        // run pig in local mode
        pigServer = new PigServer(ex);
        try {
            pigServer.registerJar(jatPath);
        } catch (IOException e) {
            e.printStackTrace();
        }


        pigServer.registerQuery("basicData = LOAD '<i>' using org.eduonix.udf.PigCleanerUDF;" .replaceAll("<i>", inputPath));

        pigServer.registerQuery( "id_details = FOREACH basicData GENERATE $0 + $1  as  x, $2 +$3 as y, $4 as z ;");

        pigServer.registerQuery(
            "store id_details into '<i>' ;".replaceAll("<i>", outputPath+"/id_details"));


        pigServer.registerQuery(
                "aggregatedData = FOREACH id_details GENERATE org.eduonix.udf.PigAggregatorUDF() as (x:chararray);");



        pigServer.registerQuery(
                "store aggregatedData into '<i>' ;".replaceAll("<i>", outputPath+"/aggregatedData"));
    }

    
    
    
    
    
    
    
}
