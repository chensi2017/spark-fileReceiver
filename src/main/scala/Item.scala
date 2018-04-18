import com.fasterxml.jackson.annotation.JsonIgnoreProperties

@JsonIgnoreProperties(ignoreUnknown = true)
case class Item (allowed:String,date:String,method:String,reqUrl:String,requestTime:Long,responseTime:Long,srcIP:String,srcPort:Int,useTime:Long)

