package hash_join.join;

/**
 * Created with IntelliJ IDEA.
 * User: root
 * Date: 7/20/15
 * Time: 6:37 PM
 * To change this template use File | Settings | File Templates.
 */

import java.util.ArrayList ;
import java.util.Hashtable ;
import java.util.Iterator ;

import hash_join.realtion.* ;


public class hashjoin  extends join
{
    public hashjoin ( String outputName , Relation r1 , Relation r2 ,
                            Attribute r1JoinAttribute , Attribute r2JoinAttribute )
    {
        super(outputName , r1 , r2 , r1JoinAttribute , r2JoinAttribute);
    }

    @Override
    public int cost ()
    {
        return 0 ;
    }

    @Override
    public Relation execute ()
    {
        // this step , we merge all the attributes from two table/relation together

        ArrayList<Attribute> outputAttributes = new ArrayList<Attribute>(build.getAttributes() ) ;
        outputAttributes.addAll(probe.getAttributes())  ;

        // and here we create a new relation ,
        TemporaryRelation output = new TemporaryRelation(outputName, outputAttributes) ;

        // this is the temporary table
        Hashtable<Object,Tuple> table = new Hashtable<Object, Tuple> () ;
        Iterator<Tuple> iterator = build.iterator  () ;

        while (iterator.hasNext())
        {
            Tuple current = iterator.next() ;
            table.put( current.get( buildJoinAttribute), current ) ;
        }

        scan (output, table ) ;
        return output ;
    }

    private void scan ( TemporaryRelation output , Hashtable<Object,Tuple> table )
    {
        Iterator<Tuple> iterator = probe.iterator() ;

        while ( iterator.hasNext())
        {
            Tuple current = iterator.next() ;

            if ( table.containsKey( current.get(probeJoinAttribute)))
            {
                Tuple joinedTuple = new Tuple(table.get(current.get(probeJoinAttribute))) ;
                joinedTuple.putAll( current );

                try
                {
                    output.addTuple( joinedTuple ) ;

                }
                catch ( IncompatibleNumberOfElementsException e)
                {
                    e.printStackTrace();
                }
            }
        }
    }
}
