package com.sev7e0.wow

import org.scalatest.{BeforeAndAfter, BeforeAndAfterEach, FunSuite, Matchers}

class ScalaTest extends FunSuite
	with BeforeAndAfter
	with BeforeAndAfterEach
	with Matchers{

	var str:String = _

	before{
		str = "result"
		println("当前str："+str)
	}

	after{
		println("测试结束")
	}

	test("scala test demo") {
		println(11111)
		//条件判断
		assert(1 == 1)
		//用于判断是否发生异常
		assertThrows[Exception](1/0)
		//对给定的字符串进行类型检查
		assertCompiles("""val a: String = "a" """)
		//判断结果是否与期待的结果值相同
		assertResult(str)(result)
		
		str shouldEqual "result"
	}
	def result:String ={
		"result"
	}
}


