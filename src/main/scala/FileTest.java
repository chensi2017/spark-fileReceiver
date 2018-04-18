import java.io.RandomAccessFile;

public class FileTest {
    public static void main(String[] args) throws Exception {
        RandomAccessFile r = new RandomAccessFile("D:/textFile/a.txt", "r");
        byte[] b = new byte[1024];
        long startOffset = r.length();
        System.out.println(startOffset);
        while (true) {
            long position = r.length();
            r.seek(startOffset);
            int len = (int)(position-startOffset);
            System.out.println("len:"+len);
            r.read(b,0,len);
            String str = new String(b, 0, len);
            System.out.println("str:"+str);
            int lastIndex = str.lastIndexOf("\n");
            System.out.println("lastIndex:"+lastIndex);
            if(lastIndex!=-1) {
                startOffset += (lastIndex+1);
            }
            b = new byte[1024];
            Thread.sleep(2000);
        }
//        byte[] b = new byte[1024];

    }
}
