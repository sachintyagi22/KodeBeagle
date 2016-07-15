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

package com.kodebeagle.crawler.metadata

import java.io.{File, FileInputStream, ObjectInputStream}
import java.util.UUID

import com.kodebeagle.configuration.KodeBeagleConfig
import com.kodebeagle.crawler.metadata.processor.{GitHubMetaDataBunchProcessor, GitHubMetaDataRangeProcessor}
import com.kodebeagle.crawler.GitHubApiHelper._
import com.kodebeagle.crawler.{GitHubRepoDownloader, RemoteActorMaster}
import com.kodebeagle.crawler.GitHubRepoDownloader.DownloadPublicReposMetadata
import com.kodebeagle.crawler.RemoteActorMaster.{AddGitHubRepoMetaDataDownloadTask, RequestNextGitHubRepoMetaDataDownloadTask, StartMaster}
import com.kodebeagle.logging.Logger

import scala.collection.mutable
import scala.util.{Random, Try}


object GitHubRepoMetadataDownloaderTestApp extends App with Logger {

  var since = args(0).toInt
  var to = Int.MaxValue
  Try(to = args(1).toInt)

  GitHubRepoMetadataDownloader.startGitHubRepoMetadataDownloader(since, to)

}


object GitHubRepoMetadataDownloader extends Logger {

  private val bunchProcessor: GitHubMetaDataBunchProcessor = Class.forName(KodeBeagleConfig
    .metaBunchProcessorImpl).newInstance().asInstanceOf[GitHubMetaDataBunchProcessor]

  private val rangeProcessor: GitHubMetaDataRangeProcessor = Class.forName(KodeBeagleConfig
    .metaRangeProcessorImpl).newInstance().asInstanceOf[GitHubMetaDataRangeProcessor]

  private val tempFolder = "/tmp/KogeBeagle/metadata/" + UUID.randomUUID() + "/"

  val tempdir = new File(tempFolder)

  if (!tempdir.mkdirs()) throw new Exception("Temp Folder Not Created")

  def startGitHubRepoMetadataDownloader(since: Int, to: Int): Any = {

    GitHubRepoDownloader.repoDownloader ! DownloadPublicReposMetadata(since, since, to)
    log.info("#### Processing repo metadata from " + since + " up to" + to)
    None

  }

  def getRepoIdFromRange(from: Int, since: Int, to: Int): Int = {
    val (allGithubRepos, next) = getAllGitHubRepos(since)
    log.info(s"#### Processing repo metadata for Job[$from-$to] from repo Id :${since + 1}")
    val repoMetadataList = allGithubRepos.filter(x => x("fork") == "false")

      .par.flatMap(fetchAllDetails)

    // val repoMetadataJsonList = repoMetadataList.toList

    bunchProcessor.process(repoMetadataList, from, since, to)
    log.info("#### Processed repo metadata for Job[" + from + "-" + to + "] till repo Id:" +
      (if (next < to) next else to))
    next
  }

  def handleDownloadRepoMetaData(from: Int, since: Int, to: Int): Any = {

    try {

      val next: Int = getRepoIdFromRange(from, since, to)

      if (next >= to) {
        log.info("#### Reached the range limit for Job[" + from + "-" + to +
          "] Finishing the Current Job for Repo Metadata download")

        rangeProcessor.process(from, to)

        log.info("#### Job[" + from + "-" + to +
          "] Finished")

       /* Try(RemoteActorMaster.remoteActorMaster ! RequestNextGitHubRepoMetaDataDownloadTask
                  ("" + from + "-" + to,"")) */

      } else {

        GitHubRepoDownloader.repoDownloader ! DownloadPublicReposMetadata(from, next, to)

      }
    } catch {

      case ex: Exception =>
        ex.printStackTrace()
        log.error("Got Exception [" + ex.getMessage + "] Trying to download, " +
          "waiting for other tokens")
        GitHubRepoDownloader.repoDownloader ! DownloadPublicReposMetadata(from, since, to)
    }

  }

}


object GitHubRepoMetaDataTaskTracker extends Logger {

  private val pendingTask: mutable.PriorityQueue[String] = mutable.PriorityQueue.empty[String](
    implicitly[Ordering[String]].reverse)
  private val inProgressTask: mutable.HashMap[String,String] = new mutable.HashMap[String,String]()

  def intializeMasterTaskList(): Unit = {

    this.synchronized {

      log.debug("Initializing GitHubRepoMetaDataTaskTracker!!!")

      
    }

  }

  def addTask(task: String): Unit = {

    this.synchronized {

      pendingTask.enqueue(task)

    }

  }

  def getNextTask(workerUrl: String): String = {

    this.synchronized {

      var nextTask = ""
      Try(nextTask = pendingTask.dequeue())
      if(!nextTask.eq("")) {
        inProgressTask.put(nextTask, workerUrl)
      }
      nextTask

    }
  }

  def markTaskDone(task: String): Unit = {

    inProgressTask.synchronized{

      inProgressTask.remove(task)

    }

  }

  def getTaskStatus(): String = {

    s"Inprogress:: ${inProgressTask.size} PendingTasks:: ${pendingTask.size}"

  }
}