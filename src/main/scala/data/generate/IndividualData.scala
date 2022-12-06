package data.generate

import data.schema._

import scala.annotation.tailrec
import scala.util._


object IndividualData {

  val country: List[String] = List(
    "Australia",
    "Austria",
    "Brazil",
    "Bulgaria",
    "Cyprus",
    "Denmark",
    "Egypt",
    "France",
    "Germany",
    "India",
    "Iran",
    "Italy",
    "Japan",
    "Kenya",
    "Lithuania",
    "Malaysia",
    "Mexico",
    "Nepal",
    "Nigeria",
    "Pakistan"
  )

  val alpha="abcdefghijklmnopqrstuvwxyz"

  @tailrec
  def randomName(n:Int,acc:String=""):String = {
    if(acc.length >= n) acc
    else randomName(n,acc+alpha.charAt(Random.nextInt(26)))
  }

  val randomPhoneNumber = () => s"${randomNumberBetween(100,999)}-${randomNumberBetween(100,999)}-${randomNumberBetween(1000,9999)}"

  val randomNumberBetween = (start:Int,end:Int) => start + Random.nextInt((end-start)+1)

  def randomPurchaseList(n:Int,i:Int=1,list:List[PurchaseInfo]=List[PurchaseInfo]()): List[PurchaseInfo] = {
    if(i > n) list
    else randomPurchaseList(n,i+1,list ++ List(PurchaseInfo(i,randomName(randomNumberBetween(6,15)),randomNumberBetween(10,2000))))
  }
}
