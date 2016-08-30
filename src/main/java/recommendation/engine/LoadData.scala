package recommendation.engine

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime

import scala.util.{Success, Try}

class LoadData  {

  private val conf = new SparkConf().setAppName("Recommendation engine").setMaster("local")
  private val sc = new SparkContext(conf)

  def loadUsers() : RDD[User] = {
    val textFile = sc.textFile("hdfs://localhost:54310/hadoop-data/userid-profile.tsv")

    val header = textFile.first()
    val dataWithoutHeader = textFile.filter(_ != header)

    dataWithoutHeader.map(line => {
      val fields = line.split("\\t")
      if (fields.length != 5) {
        User(fields(0), None, None, None, None)
      } else {
        val age: Option[Int] = Try(fields(2).trim.toInt) match {
          case Success(value) => Some(value)
          case _ => None
        }

        val gender = fields(1).trim match {
          case Male.value => Some(Male)
          case Female.value => Some(Female)
          case _ => None
        }
        User(fields(0), gender, age, Some(fields(3)), Some(fields(4)))
      }
      // id/gender/age/country/registered
    })
  }

  def loadUserRecentTracks(): RDD[UserTrack] = {

    val textFile = sc.textFile("hdfs://localhost:54310/hadoop-data/userid-timestamp-artid-artname-traid-traname.tsv")

    textFile.map(line => {

      // this method is placed inside map because everything used inside map has to be serializable.
      // Making it a class method will not work since LoadData class is not serializable
      // https://databricks.gitbooks.io/databricks-spark-knowledge-base/content/troubleshooting/javaionotserializableexception.html
      def parseField(value :String) : Option[String] = {
        value.trim match {
          case s: String if s.isEmpty => None
          case s: String => Some(s)
        }
      }
      val fields = line.split("\\t")

      if (fields.length != 6) {
        UserTrack(fields(0), DateTime.now(), None, None, None, None)
      } else {
        // 2009-04-08T01:53:56Z
        val timestamp = DateTime.parse(fields(1))

        UserTrack(fields(0), timestamp, parseField(fields(2)), parseField(fields(3)), parseField(fields(4)), parseField(fields(5)))
      }
    })

  }

}

object Main {
  def main(args: Array[String]) :Unit = {
    println((new LoadData).loadUserRecentTracks().getNumPartitions)

  }
}

case class User(id: String, gender: Option[Gender], age: Option[Int], country: Option[String], registeredDate: Option[String])
case class UserTrack(userId: String, time: DateTime, artistId: Option[String], artistName: Option[String], trackId: Option[String], trackName: Option[String])

trait Gender {def value: String}

case object Male extends Gender {val value = "m"}
case object Female extends Gender {val value = "f"}