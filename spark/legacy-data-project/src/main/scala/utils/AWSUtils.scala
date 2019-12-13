package utils
import awscala._, s3._


object AWSUtils {

  implicit val s3 = S3.at(Region.US_EAST_2)

//  val buckets: Seq[Bucket] = s3.buckets
//  val bucket: Bucket = s3.createBucket("unique-name-xxx")
//  val summaries: Seq[S3ObjectSummary] = bucket.objectSummaries
//
//  bucket.put("sample.txt", new java.io.File("sample.txt"))
//
//  val s3obj: Option[S3Object] = bucket.getObject("sample.txt")
//
//  s3obj.foreach { obj =>
//    obj.publicUrl // http://unique-name-xxx.s3.amazonaws.com/sample.txt
//    obj.generatePresignedUrl(DateTime.now.plusMinutes(10)) // ?Expires=....
//    bucket.delete(obj) // or obj.destroy()
//  }

}






