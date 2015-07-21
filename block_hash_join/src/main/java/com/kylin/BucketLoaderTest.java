package com.kylin;

/**
 * Created with IntelliJ IDEA.
 * User: root
 * Date: 7/21/15
 * Time: 2:55 PM
 * To change this template use File | Settings | File Templates.
 */

import java.io.* ;

import com.kylin.relation.* ;
import com.kylin.util.* ;

public class BucketLoaderTest
{
    public static void main ( String [] args ) throws Exception
    {
    String personPath = "/workspace/java/data_generator/p.csv" ;
    String orderPath = "" ;

    Bucket personBucket = new Bucket ("personBucket" , "personId ", "personName") ;

    BucketLoader bucketLoader = new BucketLoader (personPath , personBucket   ) ;


    personBucket = bucketLoader.getBucekt() ;

    personBucket.print() ;

    }
}
