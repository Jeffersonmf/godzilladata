package utils

import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.services.s3.model.{ListObjectsRequest, ObjectListing}
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}

object AWSUtils {

  val credentials = new BasicAWSCredentials(config.Environment.aws_access_key, config.Environment.aws_secret_key)
  val s3Client: AmazonS3 = AmazonS3ClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(credentials)).build()

  def getFilesInS3Bucket(bucketName: String, prefix: String): Seq[String] = {
    val objectListing: ObjectListing  = s3Client.listObjects(new ListObjectsRequest().
      withBucketName(bucketName).
      withPrefix(prefix))

    val summaries = objectListing.getObjectSummaries()
    var listFilesOnS3 = Seq[String]()

    for (count <- 0 to summaries.size()-1) {
      listFilesOnS3 = listFilesOnS3 :+ "s3a://swap-log-dna/" + summaries.get(count).getKey()
    }

    listFilesOnS3
  }
}






