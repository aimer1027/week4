package com.kylin.hashjoin.relation;

/**
 * Created with IntelliJ IDEA.
 * User: root
 * Date: 7/21/15
 * Time: 4:50 AM
 * To change this template use File | Settings | File Templates.
 */

import com.hazelcast.core.Hazelcast ;
import com.hazelcast.core.HazelcastInstance ;
import com.kylin.util.IncompatibleNumberOfElementsException;

import java.util.Map;

import java.util.HashMap ;

// this class needs to change like Map<...> = Hazelcast.getMap(...) like this
public class Tuple extends  HashMap<Attribute, Object>
{
    private static final long serialVersionUID = 1L ;

    public Tuple ( Relation format , Object ... elements)
        throws IncompatibleNumberOfElementsException
    {
        if ( format.getAttributes().size() != elements.length )
        {
            throw new IncompatibleNumberOfElementsException() ;
        }

        for ( int i = 0 ; i < elements.length ; i++ )
        {
            put(format.getAttributes().get(i) , elements[i]) ;
        }
    }

    public Tuple ( int initialCapacity )
    {
        super( initialCapacity );
    }



    public Tuple (  Map <? extends Attribute , ? extends Object> m)
    {
        super( m );
    }

    public Tuple ( int initialCapacity , float loadFactor )
    {
        super( initialCapacity , loadFactor);
    }

    public  void print ()
    {
        for (Map.Entry<Attribute, Object> entry : this.entrySet())
        {
            System.out.println(entry.getKey() +"----------------> " + entry.getValue()) ;
        }
    }


}
