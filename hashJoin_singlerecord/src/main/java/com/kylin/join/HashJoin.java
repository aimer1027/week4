package com.kylin.join;

import com.kylin.relation.Bucket;
import com.kylin.relation.Item;
import com.kylin.relation.Record;
import com.kylin.relation.TableRelation;
import com.kylin.util.BucketLoader;
import com.kylin.util.TableLoader;

import java.io.File;
import java.util.*;
import com.kylin.util.TableWriter ;

/**
 * Created with IntelliJ IDEA.
 * User: root
 * Date: 7/21/15
 * Time: 4:37 PM
 * To change this template use File | Settings | File Templates.
 */
public class HashJoin
{
    // we divide two tables as the build table and the probe table
    // the build table is the in-memory table , which organized as bucket
    // the probe table , every time we load in limit lines from probe table
    // executes hash-join , and write the result into the table structure
    // then , every time after limit line executes finished ,
    // we append the data from table structure into the result data file


    String buildDataFilePath ;
    String probeDataFilePath ;
    String joinResultFilePath ;

    long probeTableLimit ;
    TableRelation probeTable ;
    TableRelation joinResultTable ;
    Bucket buildTableBucket ;
    TableWriter resultWriter ;

    Item buildItem ;
    Item probeItem ;


    public HashJoin ( String buildPath ,String probePath ,String joinPath , long limit  ,
                      Bucket buildBucket , TableRelation probeRelation )
    {
        this.buildDataFilePath = buildPath ;
        this.probeDataFilePath = probePath ;
        this.joinResultFilePath = joinPath ;

        this.probeTableLimit = limit ;

        this.buildTableBucket = buildBucket ;
        this.probeTable = probeRelation ;

        this.resultWriter = new TableWriter( joinPath )  ;

    }

    private void join ()         // a table joins all buckets
            throws Exception
    {
        // here we join the buildTableBucket and the
        // first we should get one record from the probeTable
        // get its id and get the hash key value by the id
        // then we find out whether there one bucket's hash key value matche with it
        Iterator<Record> iterator = probeTable.iterator() ;


        while ( iterator.hasNext() )
        {
            Record probe_record = iterator.next() ;      // here we got one record from probe table
            String hash_key = this.buildTableBucket.getHashKey(probe_record.getId()) ; // calculate its hash_key



            System.out.println("probe record's hash key " + hash_key) ;

            if ( buildTableBucket.containsKey(hash_key)) // search the bucket-hash , checkout if any bucket's hash_key matches
            {
                // we get one bucket's hash_key matches

                // get in the bucket , find out if there is one record's personId matches witht he record's personId

                List<Record> valueList = this.buildTableBucket.get(hash_key) ;

                for ( Record r : valueList )
                {
                    System.out.println("build bucket record's id " + r.getId()) ;
                    r.print();
                    System.out.println("probe table record's id " + probe_record.getId()) ;
                    probe_record.print() ;

                    if ( probe_record.getId().equals( r.getId()) )
                    {
                        // we find a pair of records matches with each other
                        // here we create a new record and insert it into the result join table


                        System.out.println("matches one ") ;

                        List<Item> joinedItems = new ArrayList<Item> () ;
                        Record joinedRecord = new Record(  r ) ;

                        joinedItems.addAll( joinResultTable.getItems() ) ;

                        for ( Item item : joinedItems )
                        {
                            if ( r.get(item) != null )
                            {
                                  joinedRecord.put(item ,r.get(item) )   ;
                            }
                            else if ( probe_record.get(item) != null )
                            {
                                  joinedRecord.put(item, probe_record.get(item)) ;

                            }
                        }


                        this.joinResultTable.addRecord( joinedRecord) ;

                        System.out.println("before  "+ r +"  probe "+ probe_record ) ;
                        System.out.println("after "+ joinedRecord ) ;

                        List<Item> itemList = this.probeTable.getItems() ;
                        for ( Item item : itemList)
                            System.out.println(item.getName()) ;
                    }
                    else
                    {
                        // hash-key matches , means that part of the id matches
                        // but there is no record in the bucket id value matches with it continue

                        System.out.println("no record match to " + probe_record.getId() ) ;
                        continue ;

                    }
                }

            }
            else    // no matches , next one
            {
                System.out.println("record's hash-key does not matches with any bucket's  "+ hash_key) ;
                continue ;
            }
        }


        // if there is one matches , write both records in bucket and table into the join result table
        // if there is no one matches , go on with the next


    }

    public void exeucte (  )     throws Exception
    {
        // first we should config the output file
        // then we should config the structure of the output table
        File resultFile = new File(this.joinResultFilePath ) ;
        if ( resultFile.exists() ) resultFile.delete() ;
        resultFile.createNewFile() ;

        File buildFile  = new File (this.probeDataFilePath)  ; // we load build file to get total records number
                                                                // which determines how many time we load data in probe-table

        long buildTableRecordNum = buildFile.length() ;
        int times = (int)(buildTableRecordNum / this.probeTableLimit) ;

        // bucket loads only once

        BucketLoader bucketLoader = new BucketLoader(buildDataFilePath , this.buildTableBucket) ;
        this.buildTableBucket = bucketLoader.getBucekt() ;

        int i = 0 ;


   while ( i != times  )
   {
       // probeTable loads serveral times
       // and every time , we clear the table

       this.probeTable = new TableRelation(this.probeTable.getName() , this.probeTable.getItems()) ;

        TableLoader probeTableLoader =
                new TableLoader (probeDataFilePath , this.probeTable, this.probeTableLimit, i ) ;

        i++ ;

       this.probeTable = probeTableLoader.getTable() ;

        // here we create item list for joined table
        List<Item> resultItemList = new ArrayList<Item>() ;

        resultItemList.addAll(buildTableBucket.getItemList()) ;
        resultItemList.addAll(probeTable.getItems()) ;

        // here i need to check out the resultItemList's content


        // here we create the structure of joining result table
        this.joinResultTable = new TableRelation("joinTable" , resultItemList ) ;

            // here we begin execute join operations

               // System.out.println( this.buildTableBucket ) ;

                join()  ;


        // after each join operation , we write the joinResultTable's contents into result files


              System.out.println("\n\n\n times " + i) ;

              if ( this.joinResultTable.numberOfRecords() == 0 )
                  System.out.println("no joined results ") ;
              else
              {
                  this.resultWriter.setTable( joinResultTable );
                  this.resultWriter.write();

                  System.out.println("joined table content "+ this.joinResultTable) ;
              }
              System.out.println("lastest result is append in file " + joinResultFilePath) ;



   } // times while cycle
    }


}
