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
 */

package com.kodebeagle.crawler

import java.io.{BufferedInputStream, BufferedOutputStream, PrintWriter}

import com.kodebeagle.configuration.KodeBeagleConfig
import com.kodebeagle.crawler.GitHubApiHelper._
import com.kodebeagle.crawler.GitHubRepoDownloader.DownloadPublicReposMetadata
import com.kodebeagle.logging.Logger


object GitHubRepoMetadataDownloader extends App with Logger {

  case class RepoMetaData(repoName: String, repoId: String, repoUrl: String, login: String,
                          language: String, branch: String, fork: Boolean, stargazersCount: Long)

  GitHubRepoDownloader.repoDownloader ! DownloadPublicReposMetadata(args(0).toInt)

  def getRepoIdFromRange(since: Int): Int = {
    val (allGithubRepos, next) = getAllGitHubRepos(since)
    log.info("#### Saving repo metadata json to file")
    val repoMetadataList = allGithubRepos.filter(x => x("fork") == "false").distinct
      .flatMap(fetchAllDetails).distinct

    val repoMetadataJson = repoMetadataList.map { a => a + "\n" }
    val printWriter = new PrintWriter("" + KodeBeagleConfig.metadataDir + since + ".txt")

    repoMetadataJson.foreach { a => printWriter.write(a) }
    printWriter.close
    next
  }

  /* def toJson[T <: AnyRef <% Product](t: T, addESHeader: Boolean = true,
                                     isToken: Boolean = false): String = {
    import org.json4s._
    import org.json4s.jackson.Serialization
    import org.json4s.jackson.Serialization.write
    implicit val formats = Serialization.formats(NoTypeHints)
    val indexName = t.productPrefix.toLowerCase
    if (addESHeader) {
      s"""|{ "index" : { "_index" : "repometadata", "_type" : "typerepometadata" } }
         |""".stripMargin + write(t)
    } else "" + write(t)

  } */
}
