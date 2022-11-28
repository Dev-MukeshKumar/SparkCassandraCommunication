package data.generate

import data.schema.UserHistory
import data.generate.IndividualData._

import scala.annotation.tailrec

object TableData {
  @tailrec
  def userHistoryList(n:Int=20000,i:Int=1,list: Seq[UserHistory]=Seq[UserHistory]()): Seq[UserHistory] = {
    if(i > n) list
    else {
      val singleData = UserHistory(
        Option(country(randomNumberBetween(0, 19))),
        Option(i), Option(randomNumberBetween(18, 80)),
        Option(randomPhoneNumber()),
        randomPurchaseList(randomNumberBetween(2, 15)),
        Option(randomName(randomNumberBetween(5, 15)))
      )
      userHistoryList(n,i+1,list :+ singleData)
    }
  }
}
