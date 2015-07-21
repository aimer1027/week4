package com.kylin.relation;

/**
 * Created with IntelliJ IDEA.
 * User: root
 * Date: 7/21/15
 * Time: 9:03 AM
 *
 * implemnts the Relation interface
 * works like table in database
 */
import com.kylin.util.ElementsNotMatchException ;

import java.util.ArrayList ;
import java.util.List ;
import java.util.Iterator;


public class TableRelation implements Relation
{
    private String name ;
    private List<Item> itemList ;
    private List<Record> recordList ;

    public TableRelation ( String name , List<Item> itemList )
    {
        this.name = name ;
        this.itemList = itemList ;
        this.recordList = new ArrayList<Record>() ;

    }

    public TableRelation( String name , String ... itemNames )
    {
        this.name = name ;
        this.itemList = new ArrayList<Item> () ;
        this.recordList = new ArrayList<Record>() ;

        for ( String item : itemNames )
        {
            this.itemList.add( new Item(item)) ;
        }
    }

     @Override
     public String getName ()
     {
         // get the name of this table
         return name ;
     }

    @Override
    public List<Item> getItems ()
    {
        // return the list of item
        return this.itemList ;
    }

    @Override
    public Item getItem( String itemName )
    {
        // first find the itemList to find a item which name match with the input itemName
        // if not find one , return null
        for ( Item item : this.itemList )
        {
            if ( item.getName().equals(itemName ))
            {
                return item ;
            }
        }

        return null ;
    }

    @Override
    public boolean addRecord ( Record r ) throws ElementsNotMatchException
    {
        // r.size is how may items contained in a record
        // itemList.size is how may items contained in this table
        // if not match , do not insert the record in table

        if ( r.size() != itemList.size())
        {
            throw new ElementsNotMatchException() ;
        }

        // match insert the record into table's recordList
        return    this.recordList.add(r) ;
    }

    @Override
    public boolean addRecord ( Object ... elements )  throws ElementsNotMatchException
    {
        return addRecord( new Record( this, elements )) ;
    }

    @Override
    public boolean removeRecord ( Record r ) throws ElementsNotMatchException
    {
        // first find out whether the r is an element of this table's recordList by record's name
        // not find , not exist return
        // find , remove the element from recordList

        // but first check out if the item number is match
        if ( r.size() != this.itemList.size())
        {
            // item number not match , how to fall in love ?
            throw new ElementsNotMatchException () ;
        }

         return this.recordList.remove(r) ;


    }

    @Override
    public Iterator<Record> iterator()
    {
        // TableRelation implements Relation / Iterator<Record>
        return this.recordList.iterator() ;
    }

    @Override
    public String toString ()
    {
        String result = name +":\n" ;

        // first print the item names one by one
        for ( Item item : itemList)
        {
            result += item.getName() + "\t" ;

        }

        result += "\n" ;
        // then print the values one line by one line

        for ( Record r : this.recordList)
        {
            for ( Item item : itemList )
            {
                // get attribute name
                // get record's value by putting its key
                result += r.get(item) +"\t" ;

            }

            // another new line
            result +="\n" ;
        }

        return result ;

    }

    @Override
    public int numberOfRecords ()
    {
        return this.recordList.size() ;
    }



}
