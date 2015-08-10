package com.kylin.util;

/**
 * Created with IntelliJ IDEA.
 * User: root
 * Date: 7/21/15
 * Time: 5:03 AM
 * To change this template use File | Settings | File Templates.
 */
public class IncompatibleNumberOfElementsException extends Exception
{
    private static final long serialVersionUID = 1L ;

    public IncompatibleNumberOfElementsException()
    {
        super("Incompatible number of elements ") ;
    }
}

