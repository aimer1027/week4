package com.kylin.relation;

/**
 * Created with IntelliJ IDEA.
 * User: root
 * Date: 7/21/15
 * Time: 9:05 AM
 * Record is used to descrbe a tuple in a table s
 */

import com.kylin.util.ElementsNotMatchException ;
import com.kylin.relation.Item ;

import java.util.Iterator;
import java.util.List ;
import java.util.Map ;
import java.util.HashMap ;
import java.util.Set;
import java.util.ArrayList ;


public class Record extends HashMap<Item, Object>
{
    private static final long serialVersionUID = 1L ;
    private List<Item> itemList ;
    private String primaryKeyValue ;

    public Record ( List<Item> itemList)
    {
        this.itemList = itemList ;

    }

    public Record ( Relation format , Object ... elements )
        throws ElementsNotMatchException
    {
        this.itemList = format.getItems() ;
        if ( format.getItems().size() != elements.length )
        {
            throw new ElementsNotMatchException() ;
        }

        // if match , insert the elements into the record
        for ( int i = 0 ; i < elements.length ; i++ )
        {
            // cause the Record is subclass of HashMap , so parent's put method
            // can be called like member method
            put( format.getItems().get(i) , elements[i]) ;
        }

        this.primaryKeyValue =  (String)get(format.getPrimaryKeyItem()) ;

    }

    public Record ( Bucket format , Object ... elements )
        throws ElementsNotMatchException
    {
        this.itemList = format.getItemList() ;
        if ( format.getItemList().size() != elements.length )
        {
            throw new ElementsNotMatchException() ;
        }

        for ( int i = 0 ; i < elements.length ; i++ )
        {
            put(format.getItemList().get(i) , elements[i])  ;
        }

        this.primaryKeyValue =  (String)get(format.getPrimaryKeyItem()) ;

    }


    public Record ( Map<? extends Item , ? extends Object > map )
    {
        super( map );
    }

    // method to output what information the current Record contains
    public void print()
    {
     //   System.out.println("my id is " + getId() ) ;

        for ( Map.Entry<Item , Object> entry : this.entrySet())
        {
            System.out.print (  entry.getKey() +" : " + entry.getValue() +"  " ) ;
        }
        System.out.println() ;
    }

    // this method used to get how may items it contain
    public int getItemNumber ()
    {
        Set key = keySet() ;
        return key.size() ;
    }

    public List<Item> getRecordItemList ()
    {
     return this.itemList ;

    }



    public String getId ()
    {

        return this.primaryKeyValue ;
      /*  Set set = entrySet() ;

        Iterator iterator = set.iterator() ;

        Map.Entry<Item , Object> entry = (Map.Entry<Item, Object>)iterator.next() ;

         return (String)entry.getValue() ;
         */

    }



}
