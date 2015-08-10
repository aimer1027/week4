package com.kylin.hashjoin.relation;

/**
 * Created with IntelliJ IDEA.
 * User: root
 * Date: 7/21/15
 * Time: 5:15 AM
 * To change this template use File | Settings | File Templates.
 */

import com.hazelcast.core.Hazelcast ;
import com.hazelcast.core.HazelcastInstance ;
import com.kylin.util.IncompatibleNumberOfElementsException;

import java.util.ArrayList;
import java.util.List ;
import java.util.Iterator ;

public class TemporaryRelation  implements  Relation
{
    private String name ;
    private List<Attribute> attributes ;
    private List<Tuple> tuples ;
    HazelcastInstance hz ;

    public TemporaryRelation ( String name , List<Attribute> attributes )
    {
        hz = Hazelcast.newHazelcastInstance() ;
        this.name = name ;
        this.attributes = attributes ;
        tuples = hz.getList("tuples") ;
    }

    public TemporaryRelation ( String name , String ... attributeNames )
    {
        hz = Hazelcast.newHazelcastInstance() ;
        this.name = name ;
        this.attributes =  new ArrayList<Attribute>() ;
        this.tuples = hz.getList("tuples") ;

        for ( String attribute : attributeNames )
        {
            this.attributes.add( new Attribute(attribute) ) ;
        }
    }

    @Override // here is the method to get relation nam e
    public String getName ()
    {
        return name ;
    }

    @Override
    public List<Attribute> getAttributes ()
    {
        return this.attributes ;
    }

    @Override
    public Attribute getAttribute ( String attributeName )
    {
        for ( Attribute att : attributes )
        {
            if ( att.getName().equals(attributeName ))
                return att ;
        }

        return null ;
    }


    @Override
    public boolean addTuple ( Tuple t ) throws IncompatibleNumberOfElementsException
    {
        if ( t.size() != attributes.size() ) // if the tuple's attributes number not match table structure
        // throws exception
        {
            throw new IncompatibleNumberOfElementsException();
        }
        return tuples.add( t ) ;
    }

    @Override
    public boolean addTuple ( Object ... elements ) throws  IncompatibleNumberOfElementsException
    {
        return addTuple( new Tuple ( this, elements)) ;
    }

    @Override
    public boolean removeTuple ( Tuple t ) throws IncompatibleNumberOfElementsException
    {
        if ( t.size() != attributes.size() )
            throw new IncompatibleNumberOfElementsException() ;
        return tuples.remove(t) ;
    }

    @Override
    public int numberofTuples ()
    {
        return tuples.size() ;
    }

    @Override
    public  Iterator<Tuple> iterator ()
    {
       return tuples.iterator() ;
    }

    @Override
    public String toString ()
    {
        String result = name + ":\n" ;

        for ( Attribute attribute : attributes )
        {
            result += attribute.getName () + "\t" ;
        }

        result += "\n" ;

        for ( Tuple tuple : tuples )
        {
            for ( Attribute attribute : attributes )
            {
                result += tuple.get(attribute) +"\t"  ;
            }

            result += " \n" ;
        }

        return result ;
    }

}
