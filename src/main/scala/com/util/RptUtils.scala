package com.util

object RptUtils {
  def ReqPt(requestmode:Int,processnode:Int):List[Int]={
    if (requestmode == 1 && processnode == 1){
      List[Int](1,0,0)
    }else if(requestmode == 1 && processnode == 2){
      List[Int](1,1,0)
    }else if(requestmode == 1 && processnode == 3){
      List[Int](1,1,1)
    }else{
      List[Int](0,0,0)
    }
  }

  def clickPt(requestmode:Int,iseffective:Int):List[Int]={
    if(requestmode == 2 && iseffective == 1){
      List[Int](1,0)
    }else if(requestmode == 3 && iseffective == 1){
      List[Int](0,1)
    }else{
      List[Int](0,0)
    }
  }

  def adPt(iseffective:Int,isbilling:Int,
           isbid:Int,iswin:Int,adorderid:Int,winprice:Double,
           adpayment:Double):List[Int]={
    if(iseffective == 1 && isbilling == 1 && isbid == 1){
      if(iswin == 1 && adorderid != 0) {
        List[Int](1, 1, (winprice / 1000).toInt, (adpayment / 1000).toInt)
      }else {
        List[Int](1, 0, 0, 0)
      }
    }else{
      List[Int](0,0,0,0)
    }
  }
}
