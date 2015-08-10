package com.kylin.util;

/**
 * Created with IntelliJ IDEA.
 * User: root
 * Date: 7/21/15
 * Time: 5:43 AM
 * To change this template use File | Settings | File Templates.
 */

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.kylin.hashjoin.relation.* ;
import java.io.* ;
import java.util.List;

public class FileLoader
{
    private TemporaryRelation relation ;
    File dataFile ;

    public FileLoader ( String path , TemporaryRelation relation )
    {
        BufferedReader reader = null ;
        this.relation = relation ;
        this.dataFile = new File( path ) ;

        try
        {
            reader = new BufferedReader( new FileReader ( dataFile ))  ;

            String line ;

            while ( (line = reader.readLine()) != null )
            {
                recordLoader( line ) ;
            }
        }
        catch ( Exception e)
        {
            e.printStackTrace();
        }
    }

    private void recordLoader ( String record ) throws IncompatibleNumberOfElementsException
    {


        //System.out.println("read in "+ record ) ;
        HazelcastInstance hz = Hazelcast.newHazelcastInstance() ;

        String [] strArray = record.split(",") ;

        List<Attribute> attrList = this.relation.getAttributes() ;

        if ( strArray .length != this.relation.getAttributes().size() )
        {
            throw new IncompatibleNumberOfElementsException() ;
        }

        Tuple tuple = new Tuple ( this.relation , strArray )  ;

        this.relation.addTuple( tuple ) ;
    }

    public TemporaryRelation getRelation ()
    {
        return this.relation  ;
    }
}
