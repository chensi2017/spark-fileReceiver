import java.io.{FileNotFoundException, RandomAccessFile}

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.slf4j.LoggerFactory

class FileReceiver(filePath:String,checkFormat:(String)=>Boolean=FileReceiver.checkFormat _) extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2){
  private var raf:RandomAccessFile = _
  private val log = LoggerFactory.getLogger(classOf[FileReceiver])
  override def onStart(): Unit ={
    log.info(s"Reading file:$filePath")
    try {
      raf = new RandomAccessFile(filePath,"r")
    }catch {
      case e:FileNotFoundException=>restart(s"Error reading file:$filePath",e)
        return
    }
    log.info(s"Success read file:$filePath")
    new Thread("File Receiver"){
      override def run(): Unit ={
        receiver()
      }
    }.start()
  }

  override def onStop(): Unit ={
    synchronized{
      if(raf!=null){
        raf.close()
        raf = null
        log.info(s"Closed file:$filePath")
      }
    }
  }

  def receiver(): Unit ={
    //allot 1Mb
    var b = new Array[Byte](1024*1024)
    try{
    var startOffset = raf.length()
      while (true){
        var position = raf.length()
        raf.seek(startOffset)
        var len:Int = (position-startOffset).toInt
        raf.read(b,0,len)
        val str = new String(b,0,len)
        //分割处理每条数据
        var lastIndex = str.lastIndexOf("\n")
//        println(s"lastIndex:$lastIndex")
        //        strings.foreach(store(_))
        if(lastIndex != -1){
          val strings = str.split("\r\n")
          strings.foreach(str=>{
            if(checkFormat(str)){
              store(str)
            }
          })
          startOffset += (lastIndex + 1)
        }
        Thread.sleep(1500)
      }
    }catch {
      case e:Exception=>log.error("Error receiving data",e)
      restart("Error receiving data",e)
    }finally {
      onStop()
    }
  }
}

object FileReceiver{
  /**
    * check str format is the excataly you want
     * @param str
    * @return
    */
  def checkFormat(str:String):Boolean={
    if(str.startsWith("2")&&str.endsWith("}")){
     return true
    }
    false
  }
}