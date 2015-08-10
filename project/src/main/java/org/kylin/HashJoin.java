package org.kylin;

import java.util.*;

import java.io.* ;

/**
 * Created by root on 15-8-10.
 */
public class HashJoin
{
    private String buildTableFilePath ;
    private String probeTableFilePath ;
    private String outputResultFilePath ;

    private int buildTableAttrPosition ;
    private int probeTableAttrPosition ;


    private HashMap<String , ArrayList<String>> buildTableInMemory ;
    private HashMap<String , File> buildTableInDisk ;
    private boolean isDivided = false ;

    private HashMap<String , File> probeTable ;
    private HashMap<String , String> probeBucket ; // key : hash-key , value : record

    private HashMap<String , ArrayList<String>> joinedResultInMemory ;
    private HashMap<String, File> joinedResultInDisk ;



    // B is the table total size
    // S is the buffer size
    // M is the system available memory size


    // when M*0.8 <= S , swap operation need to be executed
    private int B , S , M ;
    private int bucketNumber ;
    private  int currentSpace = 0 ;// current spack consume

    public HashJoin ( int B , int S , int M , int bucketNumber)
    {
        this.B = B ;
        this.S = S ;
        this.M = M ;
        this.bucketNumber = bucketNumber ;

        buildTableInMemory = new HashMap<String , ArrayList<String>> () ;
        buildTableInDisk = new HashMap<String, File> () ;

        probeTable = new HashMap<String,File> () ;
        probeBucket = new HashMap<String,String> () ;

        joinedResultInDisk = new HashMap<String,File> () ;
        joinedResultInMemory = new HashMap<String, ArrayList<String>> () ;

    }

    public void setBuildTableFilePath ( String path )
    {
        this.buildTableFilePath = path ;
    }

    public void setProbeTableFilePath ( String path )
    {
        this.probeTableFilePath = path ;
    }

    public void setOutputResultFilePath ( String path )
    {
        this.outputResultFilePath = path ;
    }

    public void setBuildTableAttrPosition ( int pos )
    {
        this.buildTableAttrPosition = pos ;
    }

    public void setProbeTableAttrPosition ( int pos )
    {
        this.probeTableAttrPosition = pos ;
    }

    private String hashFunction ( String key )
    {
        int r = key.hashCode() %(this.bucketNumber-1) ;
        return String.valueOf(r) ;
    }

    private void swapBuildTable ( double percentage )
    {
        int len = (int)(percentage * this.buildTableInMemory.size()) ;

        // write the len -> size bucket into file , and remove the corresponding hash-set from the hash map


        try
        {

            // first we got the bucket's hash-key value to check out if there is corresponding file on disk

            Set<String> hash_key_set = this.buildTableInMemory.keySet();

            Iterator<String> iterator = hash_key_set.iterator();

            while (len-- != 0 && iterator.hasNext() )
            {

                String hash_key = iterator.next();
                File f;

                if ((f = this.buildTableInDisk.get(hash_key)) != null) // there already hash-key files on disk
                {
                    // set true mean that append write
                    BufferedWriter writer = new BufferedWriter( new FileWriter( f , true ))  ;

                    ArrayList<String> stringArrayList = this.buildTableInMemory.get( hash_key ) ;



                    this.buildTableInMemory.remove(hash_key) ;


                    for ( String line : stringArrayList )
                    {
                        this.currentSpace -= line.getBytes().length ;

                        writer.newLine();
                        writer.write( line ) ;
                    }

                    writer.close() ;
                }
                else
                {
                    f = new File ("build_"+hash_key+".csv") ;
                    f.createNewFile() ;

                    // write all bucket's content into the file

                    BufferedWriter writer = new BufferedWriter( new FileWriter ( f ) ) ;
                    ArrayList<String>  stringArrayList = this.buildTableInMemory.get( hash_key ) ;

                    for ( String line : stringArrayList )
                    {
                        writer.write( line );
                        writer.newLine() ;
                    }

                    writer.close () ;
                    this.buildTableInDisk.put(hash_key , f ) ;
                }


            }
        }
        catch ( IOException e )
        {
            e.printStackTrace();
        }
    }

    private void loadBuildTable()
    {
        // build table's content is memory-resident
        // every time we load a recorder from file into hash-map
        // we need updated the current S 's value
        // if we use %50*S to store the build table content
        // we need to clean the value of the HashMap , swap the value the HashSet's data
        // into the corresponding temporary file

        // every space is counted by bytes

        try
        {
            BufferedReader reader = new BufferedReader(new FileReader(this.buildTableFilePath)) ;

            String line ;


            while ( (line = reader.readLine()) != null )
            {
                int recordSize = line.getBytes().length ;

                currentSpace += recordSize ;

                if ( currentSpace >= this.S*0.8)
                {
                    // we have to move half of the hash bucket into temporary file
                    // and clear the corresponding HashMap's value , then update the currentSpace
                    // then we set the isDivided to true in order that each time we add new recorder
                    // we have to find the target record from the file

                    this.swapBuildTable(0.5); // we load half of the buckets into the temporary file

                    this.isDivided = true ;
                }

                String [] columns = line.split(",") ;
                String joinedAttr = columns [this.buildTableAttrPosition] ;

                String hash_key = this.hashFunction( joinedAttr ) ;

                ArrayList<String> stringArrayList ;
                 // first search for the memory HashMap
                 if (( stringArrayList = this.buildTableInMemory.get(hash_key)) != null )
                {
                        String newElement = joinedAttr ;

                        for ( String str : columns)
                        {
                            if ( !str.equals(joinedAttr))
                            {
                                newElement +=","+str ;
                            }
                        }

                        buildTableInMemory.get(hash_key).add(newElement) ;

                        continue;
                }

                // in memory can not target , and if disk bucket already create , we find target on disk
                if ( this.isDivided )
                {
                    // then search for the disk

                    File f ;

                    if ( (f = this.buildTableInDisk.get(hash_key)) != null )
                    {
                        BufferedWriter writer = new BufferedWriter( new FileWriter ( f , true )) ;
                        // append the file

                        writer.newLine () ;
                        writer.write( joinedAttr)  ;

                        for ( String s : columns )
                        {
                            writer.write(",") ;
                            writer.write(s);
                        }


                        writer.close() ;

                    }
                    else
                    {
                        // this means we need create a new bucket
                        // we need to calculate the current space usage to determine whether create
                        // the bucket on disk or in memory

                            if ( S <= 0.8*M && S*0.5 <= currentSpace)
                            {
                                S += 0.2*M ;
                                M -= 0.2*M ;
                            }

                            if ( S*0.5 >= currentSpace )
                            {
                                // memory
                                String newElement = joinedAttr ;

                                for ( String s : columns )
                                    newElement+=","+s ;

                                ArrayList<String> arrayList = new ArrayList<String> () ;
                                arrayList.add(newElement) ;

                                this.buildTableInMemory.put( hash_key ,arrayList ) ;

                            }
                            else
                            {
                                // disk s
                                f = new File ("build_"+hash_key+".csv") ;
                                f.createNewFile() ;

                                BufferedWriter writer = new BufferedWriter( new FileWriter( f )) ;

                                writer.write( joinedAttr) ;

                                for ( String s : columns )
                                {
                                    writer.write(",") ;
                                    writer.write(s);
                                }

                                writer.close () ;

                                this.buildTableInDisk.put( hash_key, f ) ;

                            }

                    }

                }
            }

        }
        catch ( IOException e )
        {
            e.printStackTrace();
        }

    }

    private void loadProbeTable ()
    {
        try
        {
            BufferedReader reader = new BufferedReader(new FileReader(this.probeTableFilePath));

            String line ;

            while ( (line = reader.readLine() ) != null )
            {
                String [] columns = line.split(",") ;

                String joinAttr = columns[this.probeTableAttrPosition] ;
                String hashKey  = hashFunction (joinAttr) ;

                File f ;

                if ( (f=this.probeTable.get( hashKey ))  == null )
                {
                    f = new File ("probe_"+hashKey+".csv") ;
                    f.createNewFile() ;

                    BufferedWriter writer = new BufferedWriter ( new FileWriter( f )) ;

                    writer.write( joinAttr ) ; // first the join attribute field
                    for ( String s : columns )
                    {
                        if ( !s.equals( joinAttr ))
                        {
                            writer.write(",") ;
                            writer.write( s );
                        }
                    }
                    writer.close() ;

                    this.probeTable.put(joinAttr , f ) ;
                }
                else
                {
                    // here true means append
                    BufferedWriter writer = new BufferedWriter( new FileWriter( f , true )) ;

                    writer.newLine();
                    writer.write( joinAttr) ;

                    for ( String s : columns )
                    {
                        writer.write(",") ;
                        writer.write(s) ;
                    }

                    writer.close () ;

                }
            }

            reader.close () ; // after load all the data into probe-table-file
            // we close the data source

        }
        catch ( IOException e )
        {
            e.printStackTrace();
        }

    }

    private void execute () throws IOException
    {
        loadBuildTable() ;
        loadProbeTable();

        // loop for the probe table hash map

        Set<String> hashKeySet = this.probeTable.keySet() ;
        Iterator<String> iter = hashKeySet.iterator() ;

        while ( iter.hasNext()) {
            String hashKey = iter.next();

            File f = this.probeTable.get(hashKey);

            BufferedReader reader = new BufferedReader(new FileReader(f));

            String line;

            while ((line = reader.readLine()) != null)
            {
                String [] columns = line.split(",") ;

                String str = new String("");

                for (int i = 1 ; i <columns.length ; i++ )
                {
                   str += columns[i] ;

                    if ( i != columns.length-1)
                        str+="," ;
                }

                this.probeBucket.put(columns[0] , str ) ;
            }

            // here we got a bucket in memory , let do hash join with build table

            // first search the build table in memory

            // if not find , check there is divided if true search the build table in disk

            // not find yet ---> continue discard the bucket directly

            // before produce output relation , check the current space usage
            // 1. enough ===> write the data inside the memory
            // 2. not enough ===> call swap methods  .
            // write the memory output result into the disk
             // still not enough , we have to write the result on disk directly


        }

    }

    public static void main ( String [] args)
    {
        String probeTablePath = "/workspace/java/project/src/main/resources/people.csv" ;

        HashJoin join = new HashJoin (1,2,3, 4) ;

        join.setProbeTableFilePath( probeTablePath );
        join.setProbeTableAttrPosition(0);


        join.loadProbeTable();
    }
}
