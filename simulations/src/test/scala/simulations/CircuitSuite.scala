package simulations

import org.scalatest.FunSuite

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CircuitSuite extends CircuitSimulator with FunSuite {
  val InverterDelay = 1
  val AndGateDelay = 3
  val OrGateDelay = 5
  
  test("andGate example") {
    val in1, in2, out = new Wire
    andGate(in1, in2, out)
    in1.setSignal(false)
    in2.setSignal(false)
    run
    
    assert(out.getSignal === false, "and 1")

    in1.setSignal(true)
    run
    
    assert(out.getSignal === false, "and 2")

    in2.setSignal(true)
    run
    
    assert(out.getSignal === true, "and 3")
  }

  test("orGate") {
    val in1, in2, out = new Wire
    orGate(in1, in2, out)
    in1.setSignal(false)
    in2.setSignal(false)
    run
    
    assert(out.getSignal === false, "false | false = false")
    
    in1.setSignal(true)
    run
    
    assert(out.getSignal === true, "true | false = false")
    
    in2.setSignal(true)
    run
    
    assert(out.getSignal === true, "true | true = false")
    
    in1.setSignal(false)
    run
    
    assert(out.getSignal === true, "false | true = true")
  }
  
  test("orGate2") {
    val in1, in2, out = new Wire
    orGate2(in1, in2, out)
    in1.setSignal(false)
    in2.setSignal(false)
    run
    
    assert(out.getSignal === false, "false | false = false")
    
    in1.setSignal(true)
    run
    
    assert(out.getSignal === true, "true | false = false")
    
    in2.setSignal(true)
    run
    
    assert(out.getSignal === true, "true | true = false")
    
    in1.setSignal(false)
    run
    
    assert(out.getSignal === true, "false | true = true")
  }
  
  test("demux") {
    val in, c, out1, out0 = new Wire
    demux(in, List(c), List(out1, out0))
    in.setSignal(false)
    c.setSignal(false)
    run
    
    assert(out0.getSignal === false, "in = false, c = false => out0 = false")
    assert(out1.getSignal === false, "in = false, c = false => out1 = false")
    
    in.setSignal(true)
    run
    
    assert(out0.getSignal === true, "in = true, c = false => out0 = true")
    assert(out1.getSignal === false, "in = true, c = false => out1 = false")
    
    c.setSignal(true)
    run
    
    assert(out0.getSignal === false, "in = true, c = true => out0 = false")
    assert(out1.getSignal === true, "in = true, c = true => out1 = true")
    
    in.setSignal(false)
    run
    
    assert(out0.getSignal === false, "in = false, c = true => out0 = false")
    assert(out1.getSignal === false, "in = false, c = true => out1 = false")
  }

}
