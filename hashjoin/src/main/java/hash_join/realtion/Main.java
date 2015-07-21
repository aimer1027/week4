package hash_join.realtion;

/**
 * Created with IntelliJ IDEA.
 * User: root
 * Date: 7/20/15
 * Time: 12:24 PM
 * To change this template use File | Settings | File Templates.
 */

import hash_join.join.hashjoin;
import hash_join.realtion.*;
import hash_join.util.* ;

import java.io.File;


public class Main
{
    public static void main ( String [] args )
    {
         TemporaryRelation Person = new TemporaryRelation("Person" ,"personId" , "name") ;
         TemporaryRelation Order  = new TemporaryRelation( "Order","orderId" , "personOrderedId", "amount") ;


          String personDataPath = "/workspace/java/data_generator/p.csv" ;
          String orderDataPath = "/workspace/java/data_generator/o.csv" ;
          FileLoader ploader = new FileLoader( personDataPath , Person ) ;
          FileLoader oloader = new FileLoader(orderDataPath , Order) ;


          Person = ploader.getRelation() ;

     //     System.out.println( Person );
     //     System.out.println( Order );

        //public hashjoin ( String outputName , Relation r1 , Relation r2 ,           Attribute r1JoinAttribute ,
        //Attribute r2JoinAttribute )
            hashjoin HashJoin = new hashjoin("join_result" , Person , Order ,
                    Person.getAttribute("personId"),
                    Order.getAttribute("personOrderedId")) ;

           Relation result =  HashJoin.execute()  ;


           System.out.println("here is the hash join result ") ;
           System.out.println(result) ;
    }

}
