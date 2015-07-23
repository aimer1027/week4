package com.kylin.relation;

import java.io.Serializable;

/**
 * Created with IntelliJ IDEA.
 * User: root
 * Date: 7/21/15
 * Time: 9:04 AM
 *
 * Item is used to describe the attribute name of each table tuple
 *
 * Table
 * attributes   item1       | item2    | item3     | ...
 * tuple1        (value1    | value2   | value3    | ... )
 * tuple2       ( value1    | value 2  | value3    | ... )
 */
public class Item implements Serializable
{
    private String name ;

    public Item ( String name )
    {
        this.name = name ;
    }

    public String getName ()
    {
        return name ;
    }

    @Override
    public String toString ()
    {
        return getName()  ;
    }
}
