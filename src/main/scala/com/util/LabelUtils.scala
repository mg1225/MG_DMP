package com.util

object LabelUtils {
  def adLebal_LC(adspacetype:Int)={
    if(adspacetype < 10){
      ("LC0"+adspacetype->1)
    }else{
      ("LC"+adspacetype->1)
    }
  }
  def adLebal_LN(adspacetypename:String)= {
    ("LN"+adspacetypename->1)
  }
  def appLebal(appname:String)={
    ("APP"+appname,1)
  }
  def channelLabel(adplatformproviderid:Int)={
    ("CN"+adplatformproviderid,1)
  }
}
