package hash_join.join;

/**
 * Created with IntelliJ IDEA.
 * User: root
 * Date: 7/20/15
 * Time: 6:23 PM
 * To change this template use File | Settings | File Templates.
 */
import hash_join.realtion.* ;


public abstract class join
{
    protected String outputName ;
    protected Relation build , probe ;
    protected Attribute buildJoinAttribute, probeJoinAttribute ;

    public join ( String outputName , Relation r1 , Relation r2 , Attribute r1JoinAttribute , Attribute r2JoinAttribute )
    {
        this.outputName = outputName ;

        if ( r1.numberofTuples() <= r2.numberofTuples())
        {
             // build always contain less tuples
            build = r1 ;
            probe = r2 ;
            buildJoinAttribute = r1JoinAttribute ;
            probeJoinAttribute = r2JoinAttribute ;
        }

        else
        {
            build = r2 ;
            probe = r1 ;
            buildJoinAttribute = r2JoinAttribute ;
            buildJoinAttribute = r1JoinAttribute ;
        }

    }

    public abstract int cost () ;

    // execute method return an instance of Relation
    // as the result of the hash-join
    public abstract Relation execute () ;

}
