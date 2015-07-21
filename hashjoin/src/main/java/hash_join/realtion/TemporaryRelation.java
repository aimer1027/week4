package hash_join.realtion;

/**
 * Created with IntelliJ IDEA.
 * User: root
 * Date: 7/20/15
 * Time: 12:08 PM
 * To change this template use File | Settings | File Templates.
 */

import org.omg.CORBA.PUBLIC_MEMBER;

import java.util.ArrayList ;
import java.util.Iterator ;
import java.util.List ;

public class TemporaryRelation implements Relation
{
    private String name ;
    private List<Attribute> attributes ;
    private ArrayList<Tuple> tuples ;

    public TemporaryRelation ( String name , List<Attribute> attributes )
    {
        this.name = name ;
        this.attributes = attributes ;
        tuples = new ArrayList<Tuple> () ;
    }

    public TemporaryRelation ( String name , String ... attributeNames )
    {
        this.name = name ;
        this.attributes = new ArrayList<Attribute> () ;
        this.tuples = new ArrayList<Tuple> () ;
        for ( String attribute : attributeNames )
        {
            this.attributes.add( new Attribute(attribute )) ;
        }
    }

    @Override   //method to get relation name
    public String getName ()
    {
        return name ;
    }

    @Override
    public List<Attribute> getAttributes ()
    {
        return attributes ;
    }

    @Override
    public Attribute getAttribute ( String attributeName )
    {
        for ( Attribute att : attributes )
        {
            if ( att.getName().equals( attributeName))
                return att ;
        }

        return null ;
    }

    @Override
    public boolean addTuple ( Tuple t ) throws IncompatibleNumberOfElementsException
    {
      //  System.out.println( t.size() );
      //  System.out.println(attributes.size() );
        if ( t.size() != attributes.size())
            throw new IncompatibleNumberOfElementsException() ;

        return tuples.add(t) ;
    }

    @Override
    public boolean addTuple ( Object ... elements ) throws IncompatibleNumberOfElementsException
    {
        return addTuple( new Tuple (this, elements ))  ;
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
    public Iterator<Tuple> iterator ()
    {
        return tuples.iterator() ;
    }

    @Override
    public String toString()
    {
        String result = name + ":\n" ;
        for ( Attribute attribute : attributes )
        {
            result += attribute.getName() + "\t" ;
        }

        result += "\n" ;

        for ( Tuple tuple : tuples )
        {
            for ( Attribute attribute : attributes )
            {
                result += tuple.get(attribute) +"\t" ;
            }

            result += "\n" ;
        }

        return result ;
    }

}
