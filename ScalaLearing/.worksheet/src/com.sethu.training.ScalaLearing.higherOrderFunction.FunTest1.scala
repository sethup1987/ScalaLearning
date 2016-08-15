package com.sethu.training.ScalaLearing.higherOrderFunction

import com.sethu.training.ScalaLearing.higherOrderFunction.Decorator;

object FunTest1 {;import org.scalaide.worksheet.runtime.library.WorksheetSupport._; def main(args: Array[String])=$execute{;$skip(193); 
  println("Welcome to the Scala worksheet");$skip(51); 
  
    def apply (fun:Int =>String,v:Int) = fun(v);System.out.println("""apply: (fun: Int => String, v: Int)String""");$skip(43); 
  var decorator1 = new  Decorator("[","]");System.out.println("""decorator1  : com.sethu.training.ScalaLearing.higherOrderFunction.Decorator = """ + $show(decorator1 ));$skip(37); val res$0 = 
  println(apply(decorator.layout,8));System.out.println("""res0: <error> = """ + $show(res$0))}
  
}
