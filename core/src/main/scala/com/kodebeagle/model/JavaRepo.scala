/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * thiTs work for additional information regarding copyright ownership.
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

import com.kodebeagle.indexer.{ExternalTypeReference, FileMetaData, TypeReference}
import com.kodebeagle.javaparser.JavaASTParser.ParseType
import com.kodebeagle.javaparser.{JavaASTParser, SingleClassBindingResolver}
import com.kodebeagle.logging.Logger
import org.eclipse.jdt.core.dom.ASTNode

class JavaRepo(baseRepo: GithubRepo) extends Repo with Logger
  with LazyLoadSupport {

  // TODO: This constants needs to go somewhere else
  private val JAVA_LANGUAGE = "java"

  override def files: List[JavaFileInfo] = {
    if (languages.contains(JAVA_LANGUAGE)) {
      baseRepo.files
        .filter(_.fileName.endsWith(".java"))
        .map(f => new JavaFileInfo(f))
    } else {
      Nil
    }
  }

  override def statistics: JavaRepoStatistics = new JavaRepoStatistics(baseRepo.statistics)

  override def languages: Set[String] = baseRepo.languages
}

class JavaFileInfo(baseFile: FileInfo) extends FileInfo with LazyLoadSupport {

  assert(baseFile.fileName.endsWith(".java"),
    s"A java file is expected. Actual file: ${baseFile.fileName}")

  private var _searchableRefs: Option[Set[TypeReference]] = None

  private var _fileMetaData: Option[Set[FileMetaData]] = None

  private var _imports: Option[Set[String]] = None

  def searchableRefs: Set[TypeReference] = {
    getOrCompute(_searchableRefs, () => {
      parse()
      _searchableRefs.get
    })
  }

  def fileMetaData: Set[FileMetaData] = {
    getOrCompute(_fileMetaData, () => {
      parse()
      _fileMetaData.get
    })
  }

  def imports: Set[String] = {
    getOrCompute(_imports, () => {
      parse()
      _imports.get
    })
  }

  def parse(): (Set[TypeReference], Set[FileMetaData]) = {
    import scala.collection.JavaConversions._

    val parser: JavaASTParser = new JavaASTParser(true)
    val cu: ASTNode = parser.getAST(fileContent, ParseType.COMPILATION_UNIT)
    val scbr: SingleClassBindingResolver  = new SingleClassBindingResolver(cu)
    scbr.resolve()

    val nodeVsType = scbr.getTypesAtPosition
    val score = ???
    val types = ???
    val externalTypeRef = ExternalTypeReference(repoId, fileName, types, score)

    _imports = Option(scbr.getImports.toSet)
    // _searchableRefs = Option(externalTypeRef)

    _fileMetaData = ???
    ???
  }

  def isTestFile(): Boolean = imports.exists(_.contains("org.junit"))

  override def fileName: String = baseFile.fileName

  override def sloc: Int = baseFile.sloc

  override def fileContent: String = baseFile.fileContent

  override def language: String = baseFile.language

  override def repoId: Int = baseFile.repoId

  override def fileLocation: String = baseFile.fileLocation
}

class JavaRepoStatistics(repoStatistics: RepoStatistics) extends RepoStatistics {

  override def sloc: Int = repoStatistics.sloc

  override def fileCount: Int = repoStatistics.fileCount

  override def size: Long = repoStatistics.size
}
