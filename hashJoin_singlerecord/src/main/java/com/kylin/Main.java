package com.kylin;

import com.kylin.join.HashJoin;
import com.kylin.relation.Bucket;
import com.kylin.relation.TableRelation;

/**
 * Created with IntelliJ IDEA.
 * User: root
 * Date: 7/23/15
 * Time: 2:54 AM

 Hash-Join executor 's main method

 */
public class Main {

    public static void main ( String [] args ) throws Exception
    {
        String personPath = "/workspace/java/data_generator/p.csv" ;
        String orderPath = "/workspace/java/data_generator/o.csv" ;
        String resultPath = "/tmp/joined.csv" ;

        int limition_lines = 100 ;

        // here we define the bucket 's structure and bucket's name
        // bucket load once
        Bucket personBucket = new Bucket ("personBucket" , "personId ", "personName") ;

        // here we define the probe table's structure  and the name of the probe table
        // probe table read in several times , and the value of the variable limit is used
        // to as the maximum recorders read in limitation and how many read in data into TableRelation times
        // is determined by the probe-table's data file's recorder numbers and the limitation

        // simply speaking : times = probe-table-file / limitation
        TableRelation orderTable = new TableRelation( "orderTable" ,"personId" , "orderId" , "amount") ;

        //hash-join constructor's params:
        // personpath -- build bucket data file path
        // orderPath -- probe table data file path
        // resultPath -- joined table output data file path
        // limition_line -- maximum lines probe-table load recorders into memory each time

        // personBucket -- instance of Bucket , here only the structure of the bucket without loading data from data file
        // orderTable -- instance of  TableRelation , here only the structure of the table without loading data
        HashJoin joiner =
                new HashJoin(   personPath ,  orderPath ,  resultPath ,   limition_lines    ,  personBucket , orderTable  )  ;

        // here we execute the hash-join operation of bucket and the small probe-table
        joiner.exeucte();


    }
}
