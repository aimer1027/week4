package com.kylin.util;

/**
 * Created with IntelliJ IDEA.
 * User: root
 * Date: 7/21/15
 * Time: 9:06 AM
 *
 * Loader is used to load data records from *.csv file into current program 's memeory
 *
 * what is fuck : line reader is no use , no egg use
 *
 */
import com.kylin.relation.* ;
import com.kylin.util.* ;

import java.io.*;
import java.util.List;

public class TableLoader
{
    private TableRelation table ;
    private File dataFile ;


    public TableLoader( String path , TableRelation table )
    {
        // this constructor is the construct that load all of the data in file
        // by one time

        BufferedReader reader = null ;
        this.table = table ;
        this.dataFile = new File( path ) ;


        try
        {
            reader = new BufferedReader( new FileReader( dataFile ))  ;

            String line ;



            while (  ( line = reader.readLine()) != null )
            {
                recordLoader( line );
            }

            reader.close()  ;
        }
        catch (Exception e )
        {
            e.printStackTrace();
        }

    }

    public TableLoader ( String path , TableRelation table , long limit , int times )
    {
        // seek the limit * times as the begin read point

        BufferedReader reader = null ;
        this.table = table ;
        this.dataFile = new File( path ) ;


        try
        {
                reader = new BufferedReader( new FileReader( dataFile ))  ;

                String line ;

                long ignoreLines  = times*limit;
                System.out.println("ignored lines number " + ignoreLines ) ;

                // by this stupid method , i successfully pass some lines in data file
                while ( ignoreLines-- != 0 ) reader.readLine() ;

                while ( (limit-- != 0 ) &&( line = reader.readLine()) != null )
                {
                           recordLoader( line );
                }

            reader.close()  ;

            System.out.println( "times : " + times) ;
            System.out.println("contents : " + this.getTable());
        }
        catch (Exception e )
        {
            e.printStackTrace();
        }
    }

    private void recordLoader ( String record ) throws ElementsNotMatchException
    {

      //  System.out.println(record) ;
        String [] strArray = record.split(",") ;

        List<Item> attriList = this.table.getItems() ;



        if ( strArray.length != attriList.size() )
        {
            throw new ElementsNotMatchException() ;
        }

        Record r = new Record( this.table , strArray ) ;

        // here we create a record

        this.table.addRecord(r) ;
    }

    public TableRelation getTable ()
    {
        return this.table ;
    }
}
