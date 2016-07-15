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

import java.io.PrintWriter

import com.kodebeagle.configuration.KodeBeagleConfig
import com.kodebeagle.crawler.metadata.GitHubRepoMetadataDownloader
import com.kodebeagle.crawler.metadata.processor.GitHubMetaDataBunchProcessor

import scala.collection.parallel.immutable.ParSeq

class GitHubMetaDataLocalSplitFileCollector extends GitHubMetaDataBunchProcessor {

  override def process(metaDataBunch: ParSeq[String], from: Int, since: Int, to: Int): Unit= {

    val printWriter = new PrintWriter("" + GitHubRepoMetadataDownloader.tempdir + "/"
      + "KodeBeagleGitHubMetaData_" + from + "-" + to + "-"  + since + ".txt")

    metaDataBunch.foreach { a => printWriter.write(a + "\n") }

    printWriter.close

  }

}
