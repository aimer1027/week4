package com.kylin.hashjoin.relation;

/**
 * Created with IntelliJ IDEA.
 * User: root
 * Date: 7/21/15
 * Time: 4:46 AM
 * To change this template use File | Settings | File Templates.
 */
import com.kylin.util.IncompatibleNumberOfElementsException;

import java.util.List ;

public interface Relation extends Iterable<Tuple>
{
    String getName () ; // get name of this Relation
    List<Attribute> getAttributes() ; // get the list of attributes "id , name ,.." like this
    Attribute getAttribute (String attributeName ) ; // get Attribute object/instance by its name

    // relation just like table , and the tuple like the records stored inside table
    // if we want to inser/add a piece of tuple into the table , we fist should confirm that
    // the attributes' of tuple number and types are the same

    // if insert tuple are not match the structure of the relation 's attributes
    // throws an exception with name of IncompatibleNumberOfElementsException

    boolean addTuple( Tuple t ) throws  IncompatibleNumberOfElementsException ;

    // method following is used to add a tuple with this method addTuple ("id , name , age , ...")
    boolean addTuple( Object ... elememnts)  throws IncompatibleNumberOfElementsException;

    // returns true if t was in table , false otherwise
    // method used to remove an Tuple from a table / relation
    boolean removeTuple ( Tuple t ) throws IncompatibleNumberOfElementsException ;

    // return how many records stored in table
    int numberofTuples () ;
}
