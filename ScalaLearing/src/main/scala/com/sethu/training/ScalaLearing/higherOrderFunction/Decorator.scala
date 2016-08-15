package com.sethu.training.ScalaLearing.higherOrderFunction

class Decorator (left:String,right:String){
  def layout[A](x:A) =  left+x.toString()+right
  
}