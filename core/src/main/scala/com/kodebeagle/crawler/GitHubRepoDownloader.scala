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


import akka.actor.{Actor, ActorSystem, Props}
import com.kodebeagle.configuration.KodeBeagleConfig
import com.kodebeagle.crawler.RemoteActorMaster._
import com.kodebeagle.crawler.RemoteActorWorker.{GetNextGitHubRepoMetaDataDownloadTask, SendErrorStatus, SendStatusAndGetNewTask}
import com.kodebeagle.crawler.metadata.{GitHubRepoMetaDataTaskTracker, GitHubRepoMetadataDownloader}
import com.kodebeagle.logging.Logger


class GitHubRepoDownloader extends Actor with Logger {

  import GitHubRepoCrawlerApp._
  import GitHubRepoDownloader._

  def receive: PartialFunction[Any, Unit] = {

    case DownloadOrganisationRepos(organisation) => downloadFromOrganization(organisation)

    case DownloadJavaScriptRepos(page) =>
      val corruptRepo = JavaScriptRepoDownloader.startCrawlingFromSkippedCount(page, "")
      if (corruptRepo == "") {
        JavaScriptRepoDownloader.pageNumber = JavaScriptRepoDownloader.pageNumber + 1
      }
      self ! DownloadJavaScriptRepos(JavaScriptRepoDownloader.pageNumber)

    case DownloadPublicRepos(since, zipOrClone) =>
      try {
        val nextSince = downloadFromRepoIdRange(since, zipOrClone)
        self ! DownloadPublicRepos(nextSince, zipOrClone)
      } catch {
        case ex: Exception =>
          ex.printStackTrace()
          log.error("Got Exception [" + ex.getMessage + "] Trying to download, " +
            "waiting for other tokens")
          self ! DownloadPublicRepos(since, zipOrClone)
      }

    case RateLimit(rateLimit) =>
      log.debug(s"rate Limit Remaining is : $rateLimit")
      if (rateLimit == "0") {
        GitHubApiHelper.token = KodeBeagleConfig.nextToken()
        log.debug("limit 0,token changed :" + GitHubApiHelper.token)
      }

    case DownloadPublicReposMetadata(from, since, to) =>
      GitHubRepoMetadataDownloader.handleDownloadRepoMetaData(from, since, to)
  }

}


object GitHubRepoDownloader {


  case class DownloadOrganisationRepos(organisation: String)

  case class DownloadPublicRepos(since: Int, zipOrClone: String)

  case class DownloadPublicReposMetadata(from: Int, since: Int, to: Int)

  case class DownloadJavaScriptRepos(pageNumber: Int)

  case class RateLimit(limit: String)

  val system = ActorSystem("RepoDownloder")

  val repoDownloader = system.actorOf(Props[GitHubRepoDownloader])

  val zipActor = system.actorOf(Props[ZipActor])


}

class ZipActor extends Actor {
  def receive: PartialFunction[Any, Unit] = {
    case filePath: String => ZipUtil.createZip(filePath, filePath + ".zip")
      import sys.process._
      Process("rm -fr " + filePath).!!
  }

}

class RemoteActorMaster extends Actor with Logger {



  def receive: PartialFunction[Any, Unit] = {

    case StartMaster() =>
      GitHubRepoMetaDataTaskTracker.intializeMasterTaskList()

    case RequestNextGitHubRepoMetaDataDownloadTask(previousTask, workerUrl) =>
      if (!previousTask.equals("")) {
        GitHubRepoMetaDataTaskTracker.markTaskDone(previousTask)
      }
      val nextTask = GitHubRepoMetaDataTaskTracker.getNextTask(workerUrl)
      if (nextTask.eq("")) {

        log.debug("No More Task Available")

      }
      sender() ! GetNextGitHubRepoMetaDataDownloadTask(nextTask)

    case GetGitHubRepoMetaDataDownloadTaskStatus =>

      sender() ! GitHubRepoMetaDataTaskTracker.getTaskStatus()

    case AddGitHubRepoMetaDataDownloadTask(task) =>

      GitHubRepoMetaDataTaskTracker.addTask(task)

    case ReportErrorStatus(task) =>
      GitHubRepoMetaDataTaskTracker.restartTask(task)
  }

}

class RemoteActorWorker extends Actor with Logger {



  def receive: PartialFunction[Any, Unit] = {

    case GetNextGitHubRepoMetaDataDownloadTask(task) =>

      if (task.equals("start")) {

        log.info("Starting the GetNextGitHubRepoMetaDataDownloadTask Worker")
        RemoteActorMaster.remoteActorMaster ! RequestNextGitHubRepoMetaDataDownloadTask("", "")

      } else if (task.equals("")) {

        log.info("No Task returned from master. Retrying...")
        RemoteActorMaster.remoteActorMaster ! RequestNextGitHubRepoMetaDataDownloadTask(task, "")

      } else {

        val range = task.split("-")

        GitHubRepoMetadataDownloader.startGitHubRepoMetadataDownloader(
          range(0).toInt, range(1).toInt)

      }

    case SendStatusAndGetNewTask(previousTask) =>

      RemoteActorMaster.remoteActorMaster !
        RequestNextGitHubRepoMetaDataDownloadTask(previousTask, "")

    case SendErrorStatus(task) =>

      RemoteActorMaster.remoteActorMaster ! ReportErrorStatus(task)

      RemoteActorWorker.remoteActorWorker ! GetNextGitHubRepoMetaDataDownloadTask("start")
  }

}


object RemoteActorMaster {

  val system = ActorSystem("RemoteActorMaster")

  val remoteActorMaster = system.actorOf(Props[RemoteActorMaster], name = "remoteActorMaster")

  case class StartMaster()

  case class RequestNextGitHubRepoMetaDataDownloadTask(previousTask: String, workerUrl: String)

  case class GetGitHubRepoMetaDataDownloadTaskStatus()

  case class AddGitHubRepoMetaDataDownloadTask(task: String)

  case class ReportErrorStatus(task: String)

}

object RemoteActorWorker {

  val system = ActorSystem("RemoteActorWorker")

  val remoteActorWorker = system.actorOf(Props[RemoteActorWorker])

  case class GetNextGitHubRepoMetaDataDownloadTask(task: String)

  case class SendStatusAndGetNewTask(previousTask: String)

  case class SendErrorStatus(task: String)
}


