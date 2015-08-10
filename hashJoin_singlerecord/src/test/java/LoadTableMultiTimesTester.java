import com.kylin.relation.TableRelation;
import com.kylin.util.TableLoader;

import java.io.File;

/**
 * Created with IntelliJ IDEA.
 * User: root
 * Date: 7/23/15
 * Time: 2:23 AM
 * To change this template use File | Settings | File Templates.
 */
public class LoadTableMultiTimesTester
{
    public static void main ( String args []) throws Exception
    {
        String orderPath = "/workspace/java/data_generator/o.csv" ;


        File f =  new File (orderPath) ;

        long fileLen = f.length() ;
        int limit = 10 ;
        int times = 2 ;

        int i = 0 ;
        while ( i != times )
        {
            TableRelation orderTable = new TableRelation( "orderTable" ,"personId" , "orderId" , "amount") ;
            TableLoader loader = new TableLoader (orderPath , orderTable , limit , i) ;
            orderTable = loader.getTable() ;
            i++ ;
        }

    }
}
