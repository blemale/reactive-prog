package simulations

import math.random

class EpidemySimulator extends Simulator {

  def randomBelow(i: Int) = (random * i).toInt

  protected[simulations] object SimConfig {
    val population: Int = 300
    val roomRows: Int = 8
    val roomColumns: Int = 8
    val prevalenceRate: Double = 0.01
    val transmissionRate: Double = 0.40
    val deadRate: Double = 0.25
    val modeDuration: Int = 5
  }

  import SimConfig._

  val persons: List[Person] =
    ((for (i <- 0 until (population * prevalenceRate).toInt) yield {
      val p = new Person(i)
      p.becomeInfected
      p
    }) ++
      (for (i <- (population * prevalenceRate).toInt until population) yield new Person(i))).toList

  class Person(val id: Int) {
    var infected = false
    var sick = false
    var immune = false
    var dead = false

    // demonstrates random number generation
    var row: Int = randomBelow(roomRows)
    var col: Int = randomBelow(roomColumns)

    def ifNotDead(bloc: => Unit) {
      if (!dead) bloc
    }

    def tryToMove() = ifNotDead {
      val safeMoves = findSafeMoves
      if (!safeMoves.isEmpty) {
        changePosition(safeMoves)
        if (!infected && infectedPersonHere(row, col)) becomeInfectedOrNot
      }
    }

    def findPossibleMove =
      for {
        i <- -1 to 1
        j <- -1 to 1
        if Math.abs(i) != Math.abs(j)
      } yield ((row + roomRows + i) % roomRows, (col + roomColumns + j) % roomColumns)

    def findSafeMoves = findPossibleMove.filter { case (r, c) => !visiblyInfectedPeopleHere(r, c) }

    def changePosition(possibleMoves: Seq[(Int, Int)]) = {
      val (r, c) = possibleMoves(randomBelow(possibleMoves.length))
      row = r
      col = c
    }

    def visiblyInfectedPeopleHere(r: Int, c: Int) = persons.exists(p => p.col == c && p.row == r && (p.sick || p.dead))

    def infectedPersonHere(r: Int, c: Int) = persons.exists(p => p.col == c && p.row == r && p.infected)

    def becomeInfectedOrNot() = ifNotDead {
      if (random < transmissionRate) {
        becomeInfected
      }
    }

    def becomeInfected() = ifNotDead {
      infected = true
      afterDelay(6)(becomeSick)
    }

    def becomeSick() = ifNotDead {
      sick = true
      afterDelay(8)(dieOrNot)
    }

    def dieOrNot() = ifNotDead {
      if (random < deadRate) dead = true
      else afterDelay(2)(becomeImmune)
    }

    def becomeImmune() = ifNotDead {
      sick = false
      immune = true
      afterDelay(2)(becomHealthy)
    }

    def becomHealthy() = ifNotDead {
      immune = false
      infected = false
    }

    def mode(): Unit = ifNotDead {
      afterDelay(randomBelow(modeDuration) + 1) {
        if (dead == false) {
          tryToMove
          mode
        }
      }
    }

    mode
  }
}
