package hash_join.realtion;

/**
 * Created with IntelliJ IDEA.
 * User: root
 * Date: 7/20/15
 * Time: 12:06 PM
 * To change this template use File | Settings | File Templates.
 */
public class IncompatibleNumberOfElementsException extends Exception
{
    private static final long serialVersionUID = 1L ;

    public IncompatibleNumberOfElementsException ()
    {
        super("Incompatible number of elements ") ;
    }
}
