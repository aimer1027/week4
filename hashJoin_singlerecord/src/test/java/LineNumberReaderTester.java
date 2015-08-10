/**
 * Created with IntelliJ IDEA.
 * User: root
 * Date: 7/23/15
 * Time: 2:02 AM
 * To change this template use File | Settings | File Templates.
 */

import java.io.FileReader ;
import java.io.LineNumberReader ;
import java.io.BufferedReader ;
import java.io.File ;

public class LineNumberReaderTester
{
    public static void main ( String [] args ) throws Exception
    {
        File f = new File ("/tmp/joined.csv") ;

        BufferedReader lineReader = new BufferedReader( new FileReader ( f )) ;

        lineReader.mark(11);

        String line = lineReader.readLine() ;
        System.out.println("the no..11 line content " + line ) ;
        lineReader.close()  ;
    }
}
