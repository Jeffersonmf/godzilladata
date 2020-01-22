package utils

import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.services.s3.model.{ListObjectsRequest, ObjectListing}

object AWSUtils {

  val credentials = new BasicAWSCredentials(config.Environment.aws_access_key, config.Environment.aws_secret_key)
  val s3Client: AmazonS3 = AmazonS3ClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(credentials)).build()

  def getFilesInS3Bucket(bucketName: String, prefix: String) = {
    val objectListing: ObjectListing  = s3Client.listObjects(new ListObjectsRequest().
      withBucketName(bucketName).
      withPrefix(prefix));

    objectListing.getCommonPrefixes()
  }
}






