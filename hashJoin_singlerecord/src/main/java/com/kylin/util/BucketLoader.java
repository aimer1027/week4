package com.kylin.util;

/**
 * Created with IntelliJ IDEA.
 * User: root
 * Date: 7/21/15
 * Time: 2:03 PM
 * To change this template use File | Settings | File Templates.
 */

import java.io.* ;
import java.util.List ;

import com.kylin.relation.* ;

public class BucketLoader
{
    Bucket bucket ;
    File dataFile ;


    /*
    * @param path is used to describe where the *.csv load in file 's path
    * @param bucket is used to store the readin data information
    *
    * */
    public BucketLoader ( String path ,  Bucket bucket     )
    {
        BufferedReader reader = null ;
        this.bucket  = bucket ;
        this.dataFile = new File(path) ;

        try
        {
            reader = new BufferedReader( new FileReader ( dataFile )) ;

            String line ;

            System.out.println("total records number " + dataFile.length() ) ;
            System.out.println("total bucket number " + bucket.getBucketNumber()) ;
            System.out.println("each bucket contain records number "+ dataFile.length()/this.bucket.getBucketNumber()) ;


            while (   (line = reader.readLine()) != null )
            {

                 lineLoader(line) ;
            }

            reader.close()  ;
        }
        catch ( Exception e )
        {
            e.printStackTrace();
        }

    }

    private void lineLoader (String record) throws ElementsNotMatchException
    {
        // here we transfer the line --> TableRelation --> insert it into
        // Bucket

      //  System.out.println(record);
        String [] strArray = record.split(",") ;

        List<Item> itemList = this.bucket.getItemList() ;

        // now we get both the bucket items and the file's item , we confirm their
        // item number
     //   System.out.println( strArray.length );
     //   System.out.println(itemList.size() ) ;

        if ( strArray.length != itemList.size() )
        {
            throw new ElementsNotMatchException()  ;
        }

       Record r = new Record ( this.bucket , strArray ) ;
       // r.print();

        this.bucket.addRecorder( r );
    }

    public Bucket getBucekt ( )
    {
        return this.bucket ;
    }
}

