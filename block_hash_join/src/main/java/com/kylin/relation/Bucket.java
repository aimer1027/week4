package com.kylin.relation;

/**
 * Created with IntelliJ IDEA.
 * User: root
 * Date: 7/21/15
 * Time: 9:04 AM
 *
 * Bucket is used to describe a datastructure
 *
 * Map< hash_id , List<Map < id , TableRelation >>>
 *
 * which the hash_id is part of the id
 * and the List is used to store the TableRelation object which pait of their ids
 * are the same
 *
 * the List is the structure of the bucket
 * outer Map is the whole data ,just like this
 *
 */
import com.kylin.util.ElementsNotMatchException;
import com.sun.javafx.scene.control.TableColumnSortTypeWrapper;

import java.util.*;


public class Bucket  extends HashMap<String , List<Record>>
{
    private static final long serialVersionUID = 1L ;
    private List<Item> attributeList ;
    private String  bucketName ;
    private long bucketNumber  = 1000 ;

    public String getHashKey( String id   )
    {
        // we define the hash key is tail 4 bits  of each id
        // total record can be divided into   10^4 = 10000 buckets
        // that means bucket number = 10 ^ tail bits
        // and each bucket contains = total records / bucket number
        // if the total records is 1 0000 0000 , tail bits = 4
        // divided into 10^4 buckets , with 1 0000 records in each bucket

        long key  = Long.parseLong( id ) ;

        key %=   bucketNumber  ;

        return Long.toString(key) ;
    }


    // no no , not like that ,the hash_id is defined and calculated inside this method s
    public Bucket ( String name , String ... itemList  )
    {
        this.bucketName = name ;

        this.attributeList = new ArrayList<Item>() ;
        for ( String item : itemList )
        {
              this.attributeList.add( new Item ( item ));
        }

    }

    public void addRecorder ( Record record )  throws ElementsNotMatchException
    {
        // first , justify the element's item length match to the bucket's
        if ( this.attributeList.size() != record.getItemNumber())
            throw new ElementsNotMatchException () ;


        // calculate the element's id's hash_key
        // the TableRelation will set the id as the primary
        // we first get the record's primary key's name
        // then we use the name to get the value of the primary key



        String hash_key = getHashKey( record.getId() ) ;


        // ok , here we go the hash_key , then we justify whether the hash_key
        // has it's corresponding elements in the map
        if ( get(hash_key) == null )
        {
            // there is no value corresponding to the hash_key
            // so we should initialize a list , and insert the element into the List
            // insert the List into the map

            List<Record> bucket = new ArrayList<Record>   () ;
            bucket.add(record) ;


            put( hash_key ,bucket ) ;

        }
        else
        {
            // if not null , means there is already a TableRelation list in the HashMap
            // we just call the get method to get the List ,
            // and call the List's add method add the TableRelation inside in it

            this.get(hash_key).add(record) ;

        }

    }

    public void addBucket ( List<Record> bucket )   throws  ElementsNotMatchException
    {
        for ( Record relation : bucket )
        {
            this.addRecorder(relation); ;
        }

    }

    // this method is used to get the item structure of the bucket
    public List<Item> getItemList()
    {
        return  this.attributeList ;
    }

    public long getBucketNumber ()
    {
        return this.bucketNumber ;
    }

    public void print ()
    {

        Set<String> set = keySet() ;
        for ( String s : set )
        {
            List<Record> recordList = get(s)  ;

            System.out.println("bucket's hash key " + s ) ;
            System.out.println() ;

            for ( Record record : recordList )
            {
                record.print() ;
            }
            System.out.println( );
        }
    }



}
