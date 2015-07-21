package com.kylin.hashjoin;

/**
 * Created with IntelliJ IDEA.
 * User: root
 * Date: 7/21/15
 * Time: 5:39 AM
 * To change this template use File | Settings | File Templates.
 */

//import com.kylin.hashjoin.join.* ;
import com.kylin.hashjoin.relation.* ;
import com.kylin.util.* ;
import org.slf4j.Logger ;
import org.slf4j.LoggerFactory;


public class Main
{
    public static void main ( String [] args )
    {

    Logger logger = LoggerFactory.getLogger(Main.class) ;

     logger.debug("begin ") ;

    TemporaryRelation Person = new TemporaryRelation("Person" , "personId" , "name") ;
    TemporaryRelation Order  = new TemporaryRelation("Order" , "orderId" , "personId" , "amount") ;

    String personDataPath = "/workspace/java/data_generator/p.csv" ;
    String orderDatapath  = "/workspace/java/data_generator/o.csv" ;

    FileLoader ploader = new FileLoader(personDataPath , Person ) ;
    FileLoader oloader = new FileLoader(orderDatapath, Order ) ;

    Person = ploader.getRelation() ;

    // System.out.println( Person ) ;
        logger.debug(Person.toString());
        logger.debug("end");

    }


}
