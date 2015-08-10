package com.kylin.relation;

/**
 * Created with IntelliJ IDEA.
 * User: root
 * Date: 7/21/15
 * Time: 9:03 AM
 * relation is used as a table structure
 *
 * it contains a list of items
 * and corresponding values
 */
import java.util.List ;
import com.kylin.util.ElementsNotMatchException ;


public interface Relation extends Iterable<Record>
{
    String getName () ; // get the name of the table
    List<Item> getItems () ; // get the list of attribute "id , age, name , address .. " like this
    Item getItem ( String itemName  ) ; // get the item by given its name

    boolean addRecord( Record r)  throws ElementsNotMatchException ;
    boolean addRecord (Object ... elements ) throws ElementsNotMatchException ;
    boolean removeRecord ( Record t ) throws ElementsNotMatchException ;

    // return how may records contained in this table
    int numberOfRecords () ;
    Item getPrimaryKeyItem()  ;

}
