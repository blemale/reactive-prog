object draft {
	val roomRows = 8                          //> roomRows  : Int = 8
	val roomColumns = 8                       //> roomColumns  : Int = 8
	def findPossibleMove(row :Int, col: Int) =
      for {
        i <- -1 to 1
        j <- -1 to 1
        if (i, j) != (0, 0)
      } yield ((row + roomRows + i) % roomRows, (col + roomColumns + j) % roomColumns)
                                                  //> findPossibleMove: (row: Int, col: Int)scala.collection.immutable.IndexedSeq[
                                                  //| (Int, Int)]
  findPossibleMove(0, 0)                          //> res0: scala.collection.immutable.IndexedSeq[(Int, Int)] = Vector((7,7), (7,0
                                                  //| ), (7,1), (0,7), (0,1), (1,7), (1,0), (1,1))
  findPossibleMove(7, 7)                          //> res1: scala.collection.immutable.IndexedSeq[(Int, Int)] = Vector((6,6), (6,7
                                                  //| ), (6,0), (7,6), (7,0), (0,6), (0,7), (0,0))
  findPossibleMove(4, 4)                          //> res2: scala.collection.immutable.IndexedSeq[(Int, Int)] = Vector((3,3), (3,4
                                                  //| ), (3,5), (4,3), (4,5), (5,3), (5,4), (5,5))
}