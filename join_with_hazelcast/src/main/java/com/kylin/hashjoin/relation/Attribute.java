package com.kylin.hashjoin.relation;

import java.io.Serializable;

/**
 * Created with IntelliJ IDEA.
 * User: root
 * Date: 7/21/15
 * Time: 4:47 AM
 * To change this template use File | Settings | File Templates.
 */
public class Attribute  implements Serializable
{
    private String name ;

    public Attribute ( String name )
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
