package com.kylin;

/**
 * Created with IntelliJ IDEA.
 * User: root
 * Date: 7/21/15
 * Time: 2:55 PM
 * To change this template use File | Settings | File Templates.
 */

import java.io.* ;
import java.util.List;

import com.kylin.join.HashJoin;
import com.kylin.relation.* ;
import com.kylin.util.* ;

public class BucketLoaderTest
{
    public static void main ( String [] args ) throws Exception
    {
    String personPath = "/workspace/java/data_generator/p.csv" ;
    String orderPath = "/workspace/java/data_generator/o.csv" ;
    String resultPath = "/tmp/joined.csv" ;


   Bucket personBucket = new Bucket ("personBucket" , "personId ", "personName") ;

   TableRelation orderTable = new TableRelation( "orderTable" ,"personId" , "orderId" , "amount") ;


 /*
    BucketLoader bucketLoader = new BucketLoader (personPath , personBucket   ) ;
    TableLoader fileLoader = new TableLoader(orderPath ,orderTable )   ;
    personBucket = bucketLoader.getBucekt() ;

    personBucket.print() ;

    */

        HashJoin joiner = new HashJoin
                (   personPath ,  orderPath ,  resultPath ,   100    ,  personBucket , orderTable  )  ;

          joiner.exeucte();


    }
}
