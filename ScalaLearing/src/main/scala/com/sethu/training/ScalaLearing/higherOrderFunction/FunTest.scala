package com.sethu.training.ScalaLearing.higherOrderFunction



object FunTest  {
  def apply (fun:Int =>String,v:Int) = fun(v)
  val decorator = new  Decorator("[","]");
  println(apply(decorator.layout,8))
  
}