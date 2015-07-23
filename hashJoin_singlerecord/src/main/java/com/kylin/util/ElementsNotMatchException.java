package com.kylin.util;

/**
 * Created with IntelliJ IDEA.
 * User: root
 * Date: 7/21/15
 * Time: 9:05 AM
 *
 * ElementsNotmatchException is a subclass of Exception
 * it is thrown when a record's attributes not match to a TableRelation's
 * cause you can not inser a record into a table which their items number or type
 * is different
 */
public class ElementsNotMatchException extends Exception
{
    private static final long serialVersionUID = 1L ;

    public ElementsNotMatchException()
    {
        super("record's item number not match with table's items'") ;
    }
}
