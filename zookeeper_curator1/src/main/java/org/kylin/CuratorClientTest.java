package org.kylin;

/**
 * Created with IntelliJ IDEA.
 * User: root
 * Date: 7/24/15
 * Time: 2:36 AM
 * To change this template use File | Settings | File Templates.
 */

import org.apache.curator.framework.CuratorFramework ;
import org.apache.curator.framework.CuratorFrameworkFactory ;
import org.apache.curator.framework.CuratorFrameworkFactory.Builder ;

import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.retry.RetryNTimes ;

import java.util.List;


public class CuratorClientTest
{
    private static final String ZK_PATH = "/zktest" ;



    public static void main ( String [] args ) throws Exception
    {
        // step 1 try to connect to zookeeper server
        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder() ;

        CuratorFramework client  =  builder.connectString("127.0.0.1:2181")
                .sessionTimeoutMs(30000)
                .connectionTimeoutMs(30000)
                .canBeReadOnly(false)
                .retryPolicy( new ExponentialBackoffRetry(1000 , Integer.MAX_VALUE))
                .namespace("keeper")
                .defaultData(null)
                .build();

        client.start() ;

        System.out.println("start") ;


        // step 2 create a node on zookeeper server
        String path1= "kylin" ;
        client.create()
                .creatingParentsIfNeeded()
                .forPath("/keeper" , path1.getBytes()) ;

        print(client.getChildren().forPath("/")) ;
        print(client.getChildren().forPath("/keeper")) ;

        // here we try to get node and data
        print(client.getData().forPath("/keeper")) ;

        // here we try to modify data
        String data ="aimer" ;
        client.setData().forPath("/keeper" , data.getBytes()) ;


        // here we check out the data we set in last step
        print( client.getData().forPath("/keeper"))  ;


        // here we delete the path we create last steps
        client.delete().forPath("/keeper") ;

        // and at last , we checkout the path of the zookeeper server
        print(client.getChildren().forPath("/"));

        System.out.println( "end");
    }

    public static void print ( String ... cmds )
    {
        StringBuilder text = new StringBuilder("$") ;

        for ( String cmd : cmds )
        {
            text.append(cmd).append("  ") ;
        }

        System.out.println(text.toString() ) ;
    }

    public static void print( Object result )
    {
        System.out.println(
                result instanceof byte []
                    ? new String ((byte []) result ) : result ) ;


    }


}
