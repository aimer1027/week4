package com.kylin.util;

/**
 * Created with IntelliJ IDEA.
 * User: root
 * Date: 7/21/15
 * Time: 9:06 AM
 *
 * Loader is used to load data records from *.csv file into current program 's memeory
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
    private long limit ;


    public TableLoader ( String path , TableRelation table , long limit )
    {
        BufferedReader reader = null ;
        this.table = table ;
        this.dataFile = new File( path ) ;
        this.limit = limit ;

        try
        {
                reader = new BufferedReader( new FileReader( dataFile ))  ;

                String line ;

                while ( (this.limit-- != 0 ) &&( line = reader.readLine()) != null )
                {
                           recordLoader( line );
                }
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
