/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.kodebeagle.crawler.metadata.processor.impl

import java.io.{File, FileFilter}

import com.kodebeagle.configuration.KodeBeagleConfig
import com.kodebeagle.crawler.metadata.GitHubRepoMetadataDownloader
import com.kodebeagle.crawler.metadata.processor.GitHubMetaDataRangeProcessor
import com.kodebeagle.logging.Logger
import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter.WildcardFileFilter

import scala.collection.JavaConverters._


class GitHubMetaDataLocalCopier extends GitHubMetaDataRangeProcessor with Logger{

  override def process(from: Int, to: Int): Unit= {

    try{

      val metadataDir = KodeBeagleConfig.metadataDir

      val metadataFolder = new File(metadataDir)

      // if(!metadataFolder.mkdirs()) throw new Exception("Metadata Folder Not Created")

      val files = FileUtils.listFiles(GitHubRepoMetadataDownloader.tempdir,new WildcardFileFilter(
              s"KodeBeagleGitHubMetaData_$from-$to*.txt"), new WildcardFileFilter(
        s"KodeBeagleGitHubMetaData_$from-$to*.txt")).iterator().asScala

      for(file <- files) {
        FileUtils.deleteQuietly(new File(metadataDir + file.getName()))
        FileUtils.moveFileToDirectory(file,metadataFolder,true)

      }

    }catch {

      case ex: Exception =>
              log.error("Error while copying files from temp folder to Metadata folder", ex)
              throw new Exception("Error while copying " +
                "files from temp folder to Metadata folder",ex)
    }

  }

}
