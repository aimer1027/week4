package hash_join.realtion;

/**
 * Created with IntelliJ IDEA.
 * User: root
 * Date: 7/20/15
 * Time: 11:54 AM
 * To change this template use File | Settings | File Templates.
 */
public class Attribute
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
