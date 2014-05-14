package sprouch

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import scala.concurrent.ExecutionContext.Implicits.global

@RunWith(classOf[JUnitRunner])
class BulkActions extends FunSuite with CouchSuiteHelpers {
  import JsonProtocol._
  
  test("create, get, update, and delete documents") {
    withNewDb(db => {
      val data = Seq(Test(0, "a"), Test(1, "b"), Test(2, "c")).map(new NewDocument(_))
      for {
        bulkInserted <- db.bulkPut(data)
        newData = bulkInserted.map(doc => doc.updateData(data => data.copy(foo=data.foo+1)))
        bulkUpdated <- db.bulkPut(newData)
        bulkGotten <- db.allDocs[Test](keys=data.map(_.id))
      } yield {
        bulkGotten
      }
    })
  }
  test("get the list of errors when a update conflict happens") {
    withNewDb(db => {
      val data = Seq(Test(0, "a"), Test(1, "b"), Test(2, "c")).map(new NewDocument(_))
      for {
        bulkInserted <- db.bulkPutWithError(data)
        newData = bulkInserted._1.map(doc => doc.updateData(data => data.copy(foo = data.foo + 1)))
        bulkUpdated <- db.bulkPutWithError(newData)
        bulkConflict <- db.bulkPutWithError(bulkInserted._1)
        bulkGotten <- db.allDocs[Test](keys = data.map(_.id))
      } yield {
        assert(bulkConflict._1 === Seq())
        bulkConflict._2.zip(bulkInserted._1).foreach {
          case (error, document) =>
            assert(error.id === document.id)
            assert(error.error === "conflict")
            assert(error.reason === "Document update conflict.")
        }
        bulkGotten
      }
    })
  }
  test("get the list of errors when a create conflict happens") {
    withNewDb(db => {
      val data = Seq(Test(0, "a"), Test(1, "b"), Test(2, "c")).map(d => new NewDocument(d.foo.toString, d))
      for {
        bulkInserted <- db.bulkPutWithError(data)
        bulkConflict <- db.bulkPutWithError(data)
      } yield {
        assert(bulkConflict._1 === Seq())
        bulkConflict._2.zip(bulkInserted._1).foreach {
          case (error, document) =>
            assert(error.id === document.id)
            assert(error.error === "conflict")
            assert(error.reason === "Document update conflict.")
        }
        bulkInserted._1
      }
    })
  }
}