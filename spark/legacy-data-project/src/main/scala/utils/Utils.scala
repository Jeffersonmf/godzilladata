package utils

import java.io.File
import java.nio.file.{Files, Paths, StandardCopyOption}

import awscala.Region
import config.Environment

object Utils {

  def getListFiles(bucketName: String, prefix: String, awsRegion: Region): Seq[String] = {
    val listFilesOnS3 = AWSUtils.getFilesInS3Bucket(bucketName, prefix, awsRegion)
    listFilesOnS3
  }

  def getListLocalFiles(dir: String): List[String] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[String]()
    }

    val getNames = d.listFiles.filter(_.isFile).toList.map(x => x.getName)
    getNames
  }

  private def getListOfFilesRecursivelly(dir: String): List[File] = {
   throw new NotImplementedError()
  }

  private def moveListOfFiles(source: String, destination: String, listFiles: List[File]): Boolean = {
    try {
      for (file <- listFiles) {
        val path = Files.move(
          Paths.get(source.concat("/").concat(file.getName())),
          Paths.get(destination.concat("/").concat(file.getName())),
          StandardCopyOption.REPLACE_EXISTING
        )
      }
      true
    } catch {
      case e: Exception => {
        false
      }
    }
  }

  def isLocalSourceFolderEmpty(source: String): Boolean = {
    return if (getListLocalFiles(source).isEmpty) true else false
  }

  def isEmptyOrNull(stringValue: String): Boolean = {
    if(stringValue != null && !stringValue.isEmpty()) false else true
  }
}