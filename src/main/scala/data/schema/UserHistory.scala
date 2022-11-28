package data.schema

case class UserHistory(country:Option[String], id:Option[Int], age:Option[Int], phonenumber: Option[String], purchases: List[PurchaseInfo],username: Option[String])
