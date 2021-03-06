package hash_join.util;

/**
 * Created with IntelliJ IDEA.
 * User: root
 * Date: 7/20/15
 * Time: 12:31 PM
 * To change this template use File | Settings | File Templates.
 */
import java.io.* ;
import java.util.*;

import hash_join.realtion.* ;

public class FileLoader
{
    private TemporaryRelation relation ;

    File dataFile ;



    public FileLoader ( String path , TemporaryRelation relation )
    {
        BufferedReader reader = null ;
        this.relation = relation ;
        this.dataFile = new File ( path ) ;

        try
        {
             reader = new BufferedReader( new FileReader( dataFile))  ;


        String line ;
        while ( (line = reader.readLine()) != null )
        {
           recordLoader ( line ) ;
        }


    }
     catch ( Exception e )
     {
         e.printStackTrace();
     }

    }

    private void recordLoader ( String record ) throws IncompatibleNumberOfElementsException
    {

        // separate the elements in record line , delimiter by ','
        //      System.out.println(record) ;

        String [] strArray = record.split(",") ;

        List<Attribute> attrList =  this.relation.getAttributes() ;


      //  System.out.println(this.relation.getAttributes().size()) ;
       // System.out.println(this.relation.getAttributes().get(0).getName()) ;



        if ( strArray.length != this.relation.getAttributes().size())
        {
            throw new IncompatibleNumberOfElementsException ();
        }

        Tuple tuple = new Tuple(this.relation , strArray ) ;





         this.relation.addTuple(tuple) ;

    }

    public TemporaryRelation getRelation ()
    {
          return this.relation ;
    }
}
