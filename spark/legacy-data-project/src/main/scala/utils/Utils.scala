package utils

import java.io.File
import java.nio.file.{Files, Paths, StandardCopyOption}

object Utils {

  def getListOfFiles(dir: String): List[File]= {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }

  def getListOfFilesRecursivelly(dir: String): List[File] = {
   throw new NotImplementedError()
  }

  def moveListOfFiles(source: String, destination: String, listFiles: List[File]): Boolean = {
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

  def isSourceFolderEmpty(source: String): Boolean = {
    return if (getListOfFiles(source).isEmpty) true else false
  }

  def isEmptyOrNull(stringValue: String): Boolean = {
    if(stringValue != null && !stringValue.isEmpty()) false else true
  }
}