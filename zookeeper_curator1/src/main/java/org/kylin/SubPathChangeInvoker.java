package org.kylin;

/**
 * Created with IntelliJ IDEA.
 * User: root
 * Date: 7/24/15
 * Time: 11:02 PM
 * To change this template use File | Settings | File Templates.
 */

import org.apache.curator.utils.CloseableUtils ;
import org.apache.curator.framework.CuratorFramework ;
import org.apache.curator.framework.CuratorFrameworkFactory ;
import org.apache.curator.framework.recipes.cache.ChildData ;
import org.apache.curator.framework.recipes.cache.PathChildrenCache ;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent ;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener ;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.ZKPaths ;

public class SubPathChangeInvoker
{
    private static final String main_path = "/MainPath" ;

    public static void main ( String args [] ) throws Exception
    {
        CuratorFramework client = null ;
        PathChildrenCache cache  = null ;

        try
        {
            client  = CuratorFrameworkFactory.builder()
                            .connectString("127.0.0.1:2181")
                            .sessionTimeoutMs(3000)
                            .connectionTimeoutMs(3000)
                            .canBeReadOnly(false)
                            .retryPolicy( new ExponentialBackoffRetry(1000 , Integer.MAX_VALUE))

                            .defaultData("aimer".getBytes())
                            .build() ;
            client.start() ;

            System.out.println(" client running ") ;

            // first we create the main path
            client.create().forPath(main_path) ;


            cache = new PathChildrenCache( client , main_path , true )  ;
            cache.start() ;

            // we add the listener inside the cache
            cache.getListenable().addListener( addListenerToPathChildrenCache() );

            // the listener and the events already registered inside in the PathChildrenCache's instance
            // we let the IDE program wait for a while , and during this period we do some create, update , delete
            // by zkCli.sh
            // see what happens in this IDE program 's output
            Thread.sleep(6000);

            System.out.println("here we gonna create two sub nodes") ;
            client.create().forPath(main_path+"/node1" , "node1".getBytes()) ;
            client.create().forPath(main_path+"/node2", "node2".getBytes()) ;


            System.out.println("delete the two sub nodes , we just create ") ;
            client.delete().forPath(main_path+"/node1") ;
            client.delete().forPath(main_path+"/node2") ;


            System.out.println("here we gonna a loop for a very long time , do something on zkCli !") ;

         while( true ) ;


        }
        finally
        {
             // close and relase resources in order
              CloseableUtils.closeQuietly(cache); ;
              CloseableUtils.closeQuietly(client);

        }

    }

    private  static  PathChildrenCacheListener  addListenerToPathChildrenCache ( )
    {
        // in this method , we first create an interface object which contain events
        // invoke to different evnets , and then pass the interface : PathChildrenCacheListener
        // into the cache this class instance

        // if you refer to the api of the PathChildrenCacheListener , you will find there is one method
        // with the name of childEvent declared in it , given / write the method for this method
        // when the passed in cache finds out something happend to it the corresponding method will be called
        // by different kinds of events {child add , child node remove , child node's data updated }


        PathChildrenCacheListener listener = new PathChildrenCacheListener() {
            @Override
            public void childEvent(CuratorFramework curatorFramework, PathChildrenCacheEvent childEvent)
                    throws Exception
            {

                System.out.println("i am in childEvent ") ;

                 switch (childEvent.getType() )
                 {
                     case CHILD_ADDED:
                     {
                         System.out.println("Node added : " + ZKPaths.getNodeFromPath(childEvent.getData().getPath() )) ;
                         System.out.println("Node data is : "+ new String (childEvent.getData().getData() ) ) ;


                         Thread.sleep(1000);
                         break ;
                     }
                     case CHILD_UPDATED:
                     {
                         System.out.println("Node updated : " +ZKPaths.getNodeFromPath(childEvent.getData().getPath() )) ;
                         System.out.println("Node data is (new ): " + new String (childEvent.getData().getData())) ;


                         break ;
                     }
                     case CHILD_REMOVED:
                     {
                         System.out.println("Node removed : " +ZKPaths.getNodeFromPath(childEvent.getData().getPath())) ;
                         break ;
                     }
                 }

            }
        }     ; // new an instance of interface , remember add ";" at the end

        // we have created a listener , now we add the listener into the cache object
      return listener ;

    }
}
