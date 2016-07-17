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


import java.net.InetAddress

import akka.actor.{Actor, ActorSystem, Props}
import com.kodebeagle.configuration.KodeBeagleConfig
import com.kodebeagle.crawler.RemoteActorMaster._
import com.kodebeagle.crawler.RemoteActorWorker.{GetNextGitHubRepoMetaDataDownloadTask, SendErrorStatus, SendStatusAndGetNewTask}
import com.kodebeagle.crawler.metadata.{GitHubRepoMetaDataTaskTracker, GitHubRepoMetadataDownloader}
import com.kodebeagle.logging.{CustomConsoleAppender, Logger}
import com.typesafe.config.ConfigFactory

import scala.util.Try


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

      org.apache.log4j.Logger.getRootLogger().addAppender(CustomConsoleAppender.console)
      log.info("Master Started!!!")
      log.info("Master Listening at :: " + RemoteActorMaster.masterUrl)
      org.apache.log4j.Logger.getRootLogger().removeAppender(CustomConsoleAppender.console)

    case RequestNextGitHubRepoMetaDataDownloadTask(previousTask, workerUrl) =>
      if (!previousTask.equals("")) {
        GitHubRepoMetaDataTaskTracker.markTaskDone(previousTask)
      }
      val nextTask = GitHubRepoMetaDataTaskTracker.getNextTask(workerUrl)
      if (nextTask.eq("")) {

        log.debug("No More Task Available")

      }
      sender() ! GetNextGitHubRepoMetaDataDownloadTask(nextTask)

    case GetGitHubRepoMetaDataDownloadTaskStatus() =>

      log.info("Current Task Status :: " + metadata.GitHubRepoMetaDataTaskTracker.getTaskStatus())

      sender() ! "Current Task Status :: " + metadata.GitHubRepoMetaDataTaskTracker.getTaskStatus()

    case AddGitHubRepoMetaDataDownloadTask(task) =>

        val message = GitHubRepoMetaDataTaskTracker.addTask(task)
        if(message.length() == 0) sender() ! "Task Added :: " + task else sender() ! message

    case ReportErrorStatus(task) =>
      GitHubRepoMetaDataTaskTracker.restartTask(task)
  }

}

class RemoteActorWorker extends Actor with Logger {

  def receive: PartialFunction[Any, Unit] = {

    case GetNextGitHubRepoMetaDataDownloadTask(task) =>
      if (task.equals("start")) {
        log.info("Starting the GetNextGitHubRepoMetaDataDownloadTask Worker")
        RemoteActorWorker.remoteActorMaster !
          RequestNextGitHubRepoMetaDataDownloadTask("", Try(InetAddress.getLocalHost).toString())

      } else if (task.equals("")) {
        log.debug("No Task returned from master. Retrying...")
        RemoteActorWorker.remoteActorMaster !
          RequestNextGitHubRepoMetaDataDownloadTask(task, Try(InetAddress.getLocalHost).toString())

      } else {
        val range = task.split("-")
        GitHubRepoMetadataDownloader.startGitHubRepoMetadataDownloader(
          range(0).toInt, range(1).toInt)
      }

    case SendStatusAndGetNewTask(previousTask) =>
       RemoteActorWorker.remoteActorMaster !
        RequestNextGitHubRepoMetaDataDownloadTask(previousTask,
                  Try(InetAddress.getLocalHost).toString())

    case SendErrorStatus(task) =>
      RemoteActorWorker.remoteActorMaster ! ReportErrorStatus(task)
      RemoteActorWorker.remoteActorWorker ! GetNextGitHubRepoMetaDataDownloadTask("start")

    case RemoteActorWorker.AddGitHubRepoMetaDataDownloadTask(task) =>
      RemoteActorWorker.remoteActorMaster ! AddGitHubRepoMetaDataDownloadTask(task)

    case RemoteActorWorker.GetGitHubRepoMetaDataDownloadTaskStatus() =>
      RemoteActorWorker.remoteActorMaster ! GetGitHubRepoMetaDataDownloadTaskStatus()
      org.apache.log4j.Logger.getRootLogger().addAppender(CustomConsoleAppender.console)
      log.info("Currently processing:: " + GitHubRepoMetadataDownloader.currentTask)
      org.apache.log4j.Logger.getRootLogger().removeAppender(CustomConsoleAppender.console)

    case messagesFromMaster: String =>
      org.apache.log4j.Logger.getRootLogger().addAppender(CustomConsoleAppender.console)
      log.info(messagesFromMaster)
      org.apache.log4j.Logger.getRootLogger().removeAppender(CustomConsoleAppender.console)

  }

}


object RemoteActorMaster {

  val config = ConfigFactory.load("akka_actor_config.conf").getConfig("RemoteActorMaster")

  val system = ActorSystem("RemoteActorMaster",config)

  val remoteActorMaster = system.actorOf(Props[RemoteActorMaster], name = "remoteActorMaster")

  val masterUrl= s"akka.tcp://RemoteActorMaster@${config.
    getString("akka.remote.netty.tcp.hostname")}:${config.
    getString("akka.remote.netty.tcp.port")}/user/remoteActorMaster"

  case class StartMaster()

  case class RequestNextGitHubRepoMetaDataDownloadTask(previousTask: String, workerUrl: String)

  case class GetGitHubRepoMetaDataDownloadTaskStatus()

  case class AddGitHubRepoMetaDataDownloadTask(task: String)

  case class ReportErrorStatus(task: String)

}

object RemoteActorWorker {

  val config = ConfigFactory.load("akka_actor_config.conf").getConfig("RemoteActorWorker")

  val configMaster = ConfigFactory.load("akka_actor_config.conf").getConfig("RemoteActorMaster")

  val system = ActorSystem("RemoteActorWorker",config)

  val remoteActorWorker = system.actorOf(Props[RemoteActorWorker])

  val masterUrl= s"akka.tcp://RemoteActorMaster@${configMaster.
    getString("akka.remote.netty.tcp.hostname")}:${configMaster.
    getString("akka.remote.netty.tcp.port")}/user/remoteActorMaster"

  val remoteActorMaster = system.actorSelection(masterUrl)

  case class GetNextGitHubRepoMetaDataDownloadTask(task: String)

  case class SendStatusAndGetNewTask(previousTask: String)

  case class SendErrorStatus(task: String)

  case class AddGitHubRepoMetaDataDownloadTask(task: String)

  case class GetGitHubRepoMetaDataDownloadTaskStatus()

}


