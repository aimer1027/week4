package hash_join.realtion;

/**
 * Created with IntelliJ IDEA.
 * User: root
 * Date: 7/20/15
 * Time: 11:56 AM
 * To change this template use File | Settings | File Templates.
 */
import com.hazelcast.mapreduce.aggregation.impl.AvgTuple;

import java.util.List ;

public interface Relation extends Iterable<Tuple>
{
    String getName () ;
    List<Attribute> getAttributes () ;
    Attribute getAttribute( String attributeName ) ;

    // here the t must have the same size as relation attributes
    // returns true if t has been added , false otherwise
    boolean addTuple ( Tuple t) throws IncompatibleNumberOfElementsException  ;

    boolean addTuple( Object ... elements) throws IncompatibleNumberOfElementsException ;

    // returns true if t was in table , false otherwise
    boolean removeTuple( Tuple t ) throws IncompatibleNumberOfElementsException ;

    int numberofTuples() ;
}
