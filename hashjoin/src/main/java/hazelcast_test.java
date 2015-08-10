/**
 * Created with IntelliJ IDEA.
 * User: root
 * Date: 7/15/15
 * Time: 2:44 AM
 * To change this template use File | Settings | File Templates.
 */

import com.hazelcast.core.Hazelcast ;
import com.hazelcast.core.HazelcastInstance ;

import java.util.Map ;
import java.util.Queue ;

public class hazelcast_test
{
    public static void  main ( String [] args )
    {
        HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance() ;
        Map<Integer, String> customers = hazelcastInstance.getMap("customers") ;
        customers.put( 1 , "Joe ") ;
        customers.put(2 , "Ali") ;
        customers.put(3, "Avi") ;

        System.out.println("Customer with key 1 " + customers.get(1)) ;
        System.out.println("Map Size " + customers.size()  ) ;

        Queue<String> queueCustomers = hazelcastInstance.getQueue("customers ") ;
        queueCustomers.offer("Tom") ;
        queueCustomers.offer("Mary") ;
        queueCustomers.offer("Jane ") ;
        System.out.println("First customer "+ queueCustomers.poll() ) ;
        System.out.println("Second customer : "+ queueCustomers.peek() ) ;
        System.out.println("Queue size : "+ queueCustomers.size() ) ;

    }
}
