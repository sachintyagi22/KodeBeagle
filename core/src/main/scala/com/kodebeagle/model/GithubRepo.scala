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
package com.kodebeagle.model

import com.kodebeagle.logging.Logger
import org.apache.hadoop.conf.Configuration

/**
  * This is an abstraction over a github repo that is to be analyzed.
  *
  * By default it will read the repo from the hdfs cluster and will throw
  * RepoNotFoundException if it is not found.
  *
  * However, if update is set to `true` then it will first clone the repo
  * from Github, replace the existing repo on Hdfs and read from that repo.
  *
  * @param repoPath -- repo location e.g. "apache/spark"
  */
class GithubRepo(val configuration: Configuration, val repoPath: String)
  extends Repo with Logger with LazyLoadSupport {

  import GithubRepo._


  private var _files: Option[List[GithubFileInfo]] = None
  private var _stats: Option[RepoStatistics] = None
  private var _languages: Option[Set[String]] = None

  // TODO: How to get this? Two options:
  // 1. Read directly from from Github using repo path.
  //    (But this is likely to hit the github api rate limit)
  // 2. Keep it stored upfront and pass it along in constructor.
  val repoInfo: Option[GithubRepoInfo] = None

  init()

  def init(): Unit = {
    val repoUpdateHelper = new GithubRepoUpdateHelper(configuration, repoPath)
    if (repoUpdateHelper.shouldUpdate()) {
      repoUpdateHelper.update()
    }
    val repoFiles = repoUpdateHelper.downloadLocalFromDfs()

  }

  override def files: List[GithubFileInfo] = {
    getOrCompute(_files, () => {
      _files = Option(readProject())
      _files.get
    })
  }

  override def statistics: RepoStatistics = {
    getOrCompute(_stats, () => {
      _stats = Option(calculateStats(files))
      _stats.get
    })
  }

  override def languages: Set[String] = {
    getOrCompute(_languages, () => {
      _languages = Option(extractLanguages(files))
      _languages.get
    })
  }
}

object GithubRepo {

  case class GithubRepoInfo(login: String, id: Int, name: String, language: String,
                            defaultBranch: String, stargazersCount: Int)

  def readProject(): List[GithubFileInfo] = ???

  def calculateStats(files: List[GithubFileInfo]): RepoStatistics = ???

  def extractLanguages(files: List[GithubFileInfo]): Set[String] = ???
}

class GithubFileInfo(filePath: String) extends BaseFileInfo(filePath) {

  override def extractFileName(path: String): String = ???

  override def readFileContent(name: String): String = ???

  override def extractLang(name: String): String = ???

  override def readSloc(content: String): Int = ???

  override def fileLocation: String = ???

  override def repoId: Int = ???
}

