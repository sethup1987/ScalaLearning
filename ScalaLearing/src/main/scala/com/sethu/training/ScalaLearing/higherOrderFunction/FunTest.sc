package com.sethu.training.ScalaLearing.higherOrderFunction

import com.sethu.training.ScalaLearing.higherOrderFunction.Decorator;

object FunTest1 {
  println("Welcome to the Scala worksheet")
  
    def apply (fun:Int =>String,v:Int) = fun(v)
  var decorator1 = new  Decorator("[","]")
  println(apply(decorator.layout,8))
  
}