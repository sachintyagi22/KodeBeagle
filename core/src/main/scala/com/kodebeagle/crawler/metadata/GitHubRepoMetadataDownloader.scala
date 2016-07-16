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
import com.kodebeagle.crawler.{GitHubRepoDownloader, RemoteActorMaster, RemoteActorWorker}
import com.kodebeagle.crawler.GitHubRepoDownloader.DownloadPublicReposMetadata
import com.kodebeagle.crawler.RemoteActorMaster.{AddGitHubRepoMetaDataDownloadTask, StartMaster}
import com.kodebeagle.crawler.RemoteActorWorker.{GetGitHubRepoMetaDataDownloadTaskStatus, GetNextGitHubRepoMetaDataDownloadTask, SendErrorStatus, SendStatusAndGetNewTask}
import com.kodebeagle.logging.{CustomConsoleAppender, Logger}

import scala.collection.mutable
import scala.util.{Random, Try}


object GitHubRepoMetadataDownloaderTestApp extends App with Logger {

  var since = args(0).toInt
  var to = Int.MaxValue
  Try(to = args(1).toInt)

  GitHubRepoMetadataDownloader.startGitHubRepoMetadataDownloader(since, to)

}


object GitHubRepoMetaDataDownloaderRemoteMasterApp extends App with Logger {

  GitHubRepoMetadataDownloader.isUsingRemoteActors = true

  RemoteActorMaster.remoteActorMaster ! StartMaster()

}

object GitHubRepoMetaDataDownloaderRemoteClientShell extends App with Logger {

  GitHubRepoMetadataDownloader.isUsingRemoteActors = true

  while (true) {
    org.apache.log4j.Logger.getRootLogger().addAppender(CustomConsoleAppender.console)
    log.info("1: Check Task Status")
    log.info("2: Submit Task")
    log.info("3: Start a worker")
    org.apache.log4j.Logger.getRootLogger().removeAppender(CustomConsoleAppender.console)

    val option = scala.io.StdIn.readLine()

    option match {

      case "1" =>
        RemoteActorWorker.remoteActorWorker ! GetGitHubRepoMetaDataDownloadTaskStatus()

      case "2" =>
        org.apache.log4j.Logger.getRootLogger().addAppender(CustomConsoleAppender.console)
        log.info("Provide the Repo Id range. e.g. 0-100000")
        org.apache.log4j.Logger.getRootLogger().removeAppender(CustomConsoleAppender.console)
        RemoteActorWorker.remoteActorWorker ! RemoteActorWorker
              .AddGitHubRepoMetaDataDownloadTask(scala.io.StdIn.readLine())
      case "3" =>
        RemoteActorWorker.remoteActorWorker ! GetNextGitHubRepoMetaDataDownloadTask("start")

      case _ =>
        org.apache.log4j.Logger.getRootLogger().addAppender(CustomConsoleAppender.console)
        log.info("Invalid option")
        org.apache.log4j.Logger.getRootLogger().removeAppender(CustomConsoleAppender.console)


    }
  }
}


object GitHubRepoMetadataDownloader extends Logger {

  var isUsingRemoteActors = false

  var currentTask = ""

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

    currentTask = from + "-" + to
    try {

      val next: Int = getRepoIdFromRange(from, since, to)

      if (next >= to) {
        log.info("#### Reached the range limit for Job[" + from + "-" + to +
          "] Finishing the Current Job for Repo Metadata download")

        rangeProcessor.process(from, to)

        log.info("#### Job[" + from + "-" + to +
          "] Finished")

        if (isUsingRemoteActors) {
          RemoteActorWorker.remoteActorWorker ! SendStatusAndGetNewTask(from + "-" + to)
        }
      } else {

        GitHubRepoDownloader.repoDownloader ! DownloadPublicReposMetadata(from, next, to)

      }
      currentTask = ""
    } catch {

      case ex: GitHubTokenLimitExhaustedException =>

        log.error("Got Exception [" + ex.getMessage + "] Trying to download, " +
          "waiting for other tokens")
        GitHubRepoDownloader.repoDownloader ! DownloadPublicReposMetadata(from, since, to)

      case ex: Throwable =>

        log.error("Got Exception [" + ex.getMessage + "]", ex)
        RemoteActorWorker.remoteActorWorker ! SendErrorStatus(from + "-" + to)
        currentTask = ""
    }

  }

}


object GitHubRepoMetaDataTaskTracker extends Logger {

  private val pendingTask: mutable.PriorityQueue[String] = mutable.PriorityQueue.empty[String](
    implicitly[Ordering[String]].reverse)
  private val inProgressTask: mutable.HashMap[String, String] =
    new mutable.HashMap[String, String]()

  def addTask(task: String): String = {

    var errorFlag = true
    var returnMessage = ""
    try{
      val range = task.split("-")
      if(range(0).toInt > range(1).toInt){
        log.error("Invalid Task Parameters")
        returnMessage = "Invalid Task Parameters"
      }else if (range(0).toInt % KodeBeagleConfig.metaRepoRangeSize.toInt!=0 ||
        range(1).toInt % KodeBeagleConfig.metaRepoRangeSize.toInt!=0){
        log.error("Task range values should be multiple of " + KodeBeagleConfig.metaRepoRangeSize)
        returnMessage = "Task range values" +
          " should be multiple of " + KodeBeagleConfig.metaRepoRangeSize
      }else{
        errorFlag =false
      }
    }catch{

      case ex: Exception =>
        returnMessage = "Error :: " + ex.getMessage()
    }

    this.synchronized {

      if (!errorFlag && !pendingTask.exists(p => p.equals(task))){
        pendingTask.enqueue(task)
        log.info("Task Added :: " + task)
        returnMessage
      }

    }
    returnMessage
  }

  def getNextTask(workerUrl: String): String = {

    this.synchronized {

      var nextTask = ""
      Try(nextTask = pendingTask.dequeue())
      if (!nextTask.equals("")) {
        inProgressTask.put(nextTask, workerUrl)
      }
      nextTask

    }
  }

  def markTaskDone(task: String): Unit = {

    this.synchronized {

      inProgressTask.remove(task)

    }

  }

  def restartTask(task: String): Unit = {

    this.synchronized {

      inProgressTask.remove(task)
      if (!pendingTask.exists(p => p.equals(task))) pendingTask.enqueue(task)

    }

  }

  def getTaskStatus(): String = {

    s"Inprogress:: ${inProgressTask.size} PendingTasks:: ${pendingTask.size}"

  }
}