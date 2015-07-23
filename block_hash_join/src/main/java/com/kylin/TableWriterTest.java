package com.kylin;

import com.kylin.relation.* ;
import com.kylin.util.* ;


/**
 * Created with IntelliJ IDEA.
 * User: root
 * Date: 7/21/15
 * Time: 9:49 PM
 *
 * here is the test of table writer
 * in this class , i would like load a data file in
 * and create a TableRelation object by the data file
 *
 * then create a TableWriter and write the Table's content
 * inside in the file by TableWriter's write method
 *
 * the TableRelation object builder file path  /workspace/java/data_generator/o.csv
 * and the writer file will write the output content in path /tmp/tableWrter.csv
 */
public class TableWriterTest
{
    public static void main ( String [] args ) throws Exception
    {
    TableRelation tableRelation = new TableRelation("orderTable" , "orderId" , "personId" , "amount") ;

    String loadFilePath = "/workspace/java/data_generator/o.csv";
    String outputFilePath ="/tmp/tableWriter.csv" ;

    TableLoader tableLoader = new TableLoader(loadFilePath , tableRelation  , 100 ) ;
    tableRelation = tableLoader.getTable() ;

        TableWriter tableWriter = new TableWriter(outputFilePath , tableRelation) ;

        tableWriter.write() ;

    }
}
