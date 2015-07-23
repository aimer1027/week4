package com.kylin.util;

import com.kylin.relation.Item;
import com.kylin.relation.Record;
import com.kylin.relation.TableRelation;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.Iterator;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: root
 * Date: 7/21/15
 * Time: 9:24 PM
 *
 * in this class , append the TableRelation's content into the
 * file
 * we need append mode
 * name / path of the output file
 * instance of the TableRelation
 *
 * after all the initialization is done
 * execute write method to write the TableRelation's contents
 * into the file
 *
 * in the next step : i will change the logic of the execute
 *
 * first check the number of the records of TableRelation
 * only when its number approach the limitation write will be executed
 * */

public class TableWriter
{
    private TableRelation table ;
    String outputFilePath ;

    public TableWriter ( String path , TableRelation relation )
    {
        this.outputFilePath = path ;
        this.table = relation ;
    }

    public void write ()
    {
        try
        {
             File file = new File (this.outputFilePath) ;
             BufferedWriter output = new BufferedWriter( new FileWriter( file , true )) ;

             if ( !file.exists())
                 file.createNewFile() ;

            int recordNum = this.table.numberOfRecords() ;
            Iterator<Record> iterator = this.table.iterator() ;
            while ( iterator.hasNext())
            {
                Record r = iterator.next() ;


                List<Item> itemList = this.table.getItems() ;
                int counter = 0 ;

                for ( Item item : itemList )
                {
                    System.out.println("key " + item.getName()) ;


                    String value = r.get(item).toString() ;

                    System.out.println("value "+ value ) ;
                    output.write(value);

                     counter++ ;

                     if ( counter != itemList.size())
                              output.write(",") ;
                }
                output.newLine();

            }



            System.out.println("everything is done ") ;

        }
        catch ( Exception e)
        {
            e.printStackTrace();
        }

    }
}
