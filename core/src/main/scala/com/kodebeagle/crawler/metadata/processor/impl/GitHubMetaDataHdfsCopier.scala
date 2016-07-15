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

import java.io.File

import com.kodebeagle.configuration.KodeBeagleConfig
import com.kodebeagle.crawler.metadata.GitHubRepoMetadataDownloader
import com.kodebeagle.crawler.metadata.processor.GitHubMetaDataRangeProcessor
import com.kodebeagle.logging.Logger
import org.apache.commons.io.filefilter.WildcardFileFilter
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.commons.io.{FileUtils, IOUtils}
import scala.collection.JavaConverters._


class GitHubMetaDataHdfsCopier extends GitHubMetaDataRangeProcessor with Logger{

  override def process(from: Int, to: Int): Unit = {


    val hadoopConf = new Configuration()
    hadoopConf.set("fs.defaultFS",KodeBeagleConfig.metadataHadoopNamenode)
    val fs = FileSystem.get(hadoopConf)

    try {


      val files = FileUtils.listFiles(GitHubRepoMetadataDownloader.tempdir, new WildcardFileFilter(
        s"KodeBeagleGitHubMetaData_$from-$to*.txt"), new WildcardFileFilter(
        s"KodeBeagleGitHubMetaData_$from-$to*.txt")).iterator().asScala

      for (file: File <- files){

        val srcPath = new Path(file.getAbsolutePath())
        val destPath = new Path(KodeBeagleConfig.metadataDir + file.getName())
        fs.copyFromLocalFile(srcPath,destPath)

      }
    } catch {

      case ex: Exception =>
        log.error("Error while copying files from temp folder to Metadata folder", ex)
        throw new Exception("Error while copying " +
          "files from temp folder to Metadata folder", ex)
    }

  }

}
