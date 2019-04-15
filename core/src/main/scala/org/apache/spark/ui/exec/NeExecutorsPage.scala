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

package org.apache.spark.ui.exec

import java.net.URLEncoder
import javax.servlet.http.HttpServletRequest

import scala.util.Try
import scala.xml.Node

import org.apache.spark.status.AppStatusStore
import org.apache.spark.status.api.v1.ExecutorSummary
import org.apache.spark.ui.{SparkUITab, ToolTips, UIUtils, WebUIPage}
import org.apache.spark.util.Utils

private[ui] class NeExecutorsPage(
    parent: SparkUITab,
    store: AppStatusStore,
    threadDumpEnabled: Boolean) extends WebUIPage("") {

  // When GCTimePercent is edited change ToolTips.TASK_TIME to match
  private val GCTimePercent = 0.1

  // a safe String to Int for sorting ids (converts non-numeric Strings to -1)
  def idStrToInt(str: String) : Int = Try(str.toInt).getOrElse(-1)

  def render(request: HttpServletRequest): Seq[Node] = {
    val activeExecutorInfo = store.executorList(true).sortBy(_.id)
    val deadExecutorInfo = store.deadExecutorList().sortBy(_.id)
    val execInfoSorted = activeExecutorInfo ++ deadExecutorInfo
    val logsExist = execInfoSorted.exists(_.executorLogs.nonEmpty)

    val execTable = {
      <table class={UIUtils.TABLE_CLASS_STRIPED_SORTABLE}>
        <thead>
          <th class="sorttable_numeric">ID</th>
          <th><span data-toggle="tooltip" title={ToolTips.ADDRESS}>ADDR</span></th>
          <th><span data-toggle="tooltip" title={ToolTips.EXECUTOR_STATUS}>Status</span></th>
          <th><span data-toggle="tooltip" title={ToolTips.RDD_BLOCKS}>Blocks</span></th>
          <th><span data-toggle="tooltip" title={ToolTips.STORAGE_MEMORY}>Storage Mem</span></th>
          <th><span data-toggle="tooltip" title={ToolTips.DISK_USED}>Disk Used</span></th>
          <th><span data-toggle="tooltip" title={ToolTips.EXECUTOR_CORES}>Cores</span></th>
          <th><span data-toggle="tooltip" title={ToolTips.ACTIVE_TASKS}>ATs</span></th>
          <th><span data-toggle="tooltip" title={ToolTips.FAILD_TASKS}>FTs</span></th>
          <th><span data-toggle="tooltip" title={ToolTips.COMPLETE_TASKS}>CTs</span></th>
          <th><span data-toggle="tooltip" title={ToolTips.TOTAL_TASKS}>TTs</span></th>
          <th><span data-toggle="tooltip" title={ToolTips.TASK_TIME}>Task(GC) Time</span></th>
          <th><span data-toggle="tooltip" title={ToolTips.INPUT}>Input</span></th>
          <th><span data-toggle="tooltip" title={ToolTips.SHUFFLE_READ}>Shuf-R</span></th>
          <th><span data-toggle="tooltip" title={ToolTips.SHUFFLE_WRITE}>Shuf-W</span></th>
          <th>
            <!-- Place the shuffle write tooltip on the left (rather than the default position
              of on top) because the shuffle write column is the last column on the right side and
              the tooltip is wider than the column, so it doesn't fit on top. -->
            <span data-toggle="tooltip" data-placement="left" title={ToolTips.BLACKLISTED}>
              Blacklisted
            </span>
          </th>
          {if (logsExist) <th class="sorttable_nosort">Logs</th> else Seq.empty}
          {if (threadDumpEnabled) <th class="sorttable_nosort">TD</th> else Seq.empty}
        </thead>
        <tbody>
          {execInfoSorted.map(execRow(_, logsExist))}
        </tbody>
      </table>
    }

    val content =
      <div class="row">
        <div class="span12">
          <h4>Summary</h4>
          {execSummary(activeExecutorInfo, deadExecutorInfo)}
        </div>
      </div>
        <div class = "row">
          <div class="span12">
            <h4>Executors</h4>
            {execTable}
          </div>
        </div>

    UIUtils.headerSparkPage("NetEase Executors", content, parent)
  }

  /** Render an HTML row representing an executor */
  private def execRow(info: ExecutorSummary, logsExist: Boolean): Seq[Node] = {
    val maximumMemory = info.maxMemory
    val memoryUsed = info.memoryUsed
    val diskUsed = info.diskUsed
    val executorStatus =
      if (info.isActive) {
        "Active"
      } else {
        "Dead"
      }

    <tr>
      <td sorttable_customkey={idStrToInt(info.id).toString}>{info.id}</td>
      <td>{info.hostPort}</td>
      <td sorttable_customkey={executorStatus.toString}>
        {executorStatus}
      </td>
      <td>{info.rddBlocks}</td>
      <td sorttable_customkey={memoryUsed.toString}>
        {Utils.bytesToString(memoryUsed)} /
        {Utils.bytesToString(maximumMemory)}
      </td>
      <td sorttable_customkey={diskUsed.toString}>
        {Utils.bytesToString(diskUsed)}
      </td>
      <td>{info.totalCores}</td>
      {taskData(info.maxTasks, info.activeTasks, info.failedTasks, info.completedTasks,
      info.totalTasks, info.totalDuration, info.totalGCTime)}
      <td sorttable_customkey={info.totalInputBytes.toString}>
        {Utils.bytesToString(info.totalInputBytes)}
      </td>
      <td sorttable_customkey={info.totalShuffleRead.toString}>
        {Utils.bytesToString(info.totalShuffleRead)}
      </td>
      <td sorttable_customkey={info.totalShuffleWrite.toString}>
        {Utils.bytesToString(info.totalShuffleWrite)}
      </td>
      <td>{info.isBlacklisted.toString}</td>
      {
      if (logsExist) {
        <td>
          {
          info.executorLogs.map { case (logName, logUrl) =>
            <div>
              <a href={logUrl}>
                {logName}
              </a>
            </div>
          }
          }
        </td>
      }
      }
      {
      if (threadDumpEnabled) {
        if (info.isActive) {
          val encodedId = URLEncoder.encode(info.id, "UTF-8")
          <td>
            <a href={s"threadDump/?executorId=$encodedId"}>TD</a>
          </td>
        } else {
          <td> </td>
        }
      } else {
        Seq.empty
      }
      }
    </tr>
  }

  private def execSummaryRow(execInfo: Seq[ExecutorSummary], rowName: String): Seq[Node] = {
    val maximumMemory = execInfo.map(_.maxMemory).sum
    val memoryUsed = execInfo.map(_.memoryUsed).sum
    val diskUsed = execInfo.map(_.diskUsed).sum
    val totalCores = execInfo.map(_.totalCores).sum
    val totalInputBytes = execInfo.map(_.totalInputBytes).sum
    val totalShuffleRead = execInfo.map(_.totalShuffleRead).sum
    val totalShuffleWrite = execInfo.map(_.totalShuffleWrite).sum
    val blacklisted = execInfo.filter(_.isBlacklisted).size

    <tr>
      <td><b>{rowName}({execInfo.size})</b></td>
      <td>{execInfo.map(_.rddBlocks).sum}</td>
      <td sorttable_customkey={memoryUsed.toString}>
        {Utils.bytesToString(memoryUsed)} /
        {Utils.bytesToString(maximumMemory)}
      </td>
      <td sorttable_customkey={diskUsed.toString}>
        {Utils.bytesToString(diskUsed)}
      </td>
      <td>{totalCores}</td>
      {taskData(execInfo.map(_.maxTasks).sum,
      execInfo.map(_.activeTasks).sum,
      execInfo.map(_.failedTasks).sum,
      execInfo.map(_.completedTasks).sum,
      execInfo.map(_.totalTasks).sum,
      execInfo.map(_.totalDuration).sum,
      execInfo.map(_.totalGCTime).sum)}
      <td sorttable_customkey={totalInputBytes.toString}>
        {Utils.bytesToString(totalInputBytes)}
      </td>
      <td sorttable_customkey={totalShuffleRead.toString}>
        {Utils.bytesToString(totalShuffleRead)}
      </td>
      <td sorttable_customkey={totalShuffleWrite.toString}>
        {Utils.bytesToString(totalShuffleWrite)}
      </td>
      <td>{blacklisted}</td>
    </tr>
  }

  private def execSummary(
      activeExecInfo: Seq[ExecutorSummary],
      deadExecInfo: Seq[ExecutorSummary]): Seq[Node] = {
    val totalExecInfo = activeExecInfo ++ deadExecInfo
    val activeRow = execSummaryRow(activeExecInfo, "Active")
    val deadRow = execSummaryRow(deadExecInfo, "Dead")
    val totalRow = execSummaryRow(totalExecInfo, "Total")

    <table class={UIUtils.TABLE_CLASS_STRIPED}>
      <thead>
        <th></th>
        <th><span data-toggle="tooltip" title={ToolTips.RDD_BLOCKS}>Blocks</span></th>
        <th><span data-toggle="tooltip" title={ToolTips.STORAGE_MEMORY}>Storage Mem</span></th>
        <th><span data-toggle="tooltip" title={ToolTips.DISK_USED}>Disk Used</span></th>
        <th><span data-toggle="tooltip" title={ToolTips.EXECUTOR_CORES}>Cores</span></th>
        <th><span data-toggle="tooltip" title={ToolTips.ACTIVE_TASKS}>ATs</span></th>
        <th><span data-toggle="tooltip" title={ToolTips.FAILD_TASKS}>FTs</span></th>
        <th><span data-toggle="tooltip" title={ToolTips.COMPLETE_TASKS}>CTs</span></th>
        <th><span data-toggle="tooltip" title={ToolTips.TOTAL_TASKS}>TTs</span></th>
        <th><span data-toggle="tooltip" title={ToolTips.TASK_TIME}>Task(GC) Time</span></th>
        <th><span data-toggle="tooltip" title={ToolTips.INPUT}>Input</span></th>
        <th><span data-toggle="tooltip" title={ToolTips.SHUFFLE_READ}>Shuf-R</span></th>
        <th><span data-toggle="tooltip" title={ToolTips.SHUFFLE_WRITE}>Shuf-W</span></th>
        <th>
          <span data-toggle="tooltip" data-placement="left" title={ToolTips.BLACKLISTED}>
            Blacklisted
          </span>
        </th>
      </thead>
      <tbody>
        {activeRow}
        {deadRow}
        {totalRow}
      </tbody>
    </table>
  }

  private def taskData(
      maxTasks: Int,
      activeTasks: Int,
      failedTasks: Int,
      completedTasks: Int,
      totalTasks: Int,
      totalDuration: Long,
      totalGCTime: Long): Seq[Node] = {
    // Determine Color Opacity from 0.5-1
    // activeTasks range from 0 to maxTasks
    val activeTasksAlpha =
    if (maxTasks > 0) {
      (activeTasks.toDouble / maxTasks) * 0.5 + 0.5
    } else {
      1
    }
    // failedTasks range max at 10% failure, alpha max = 1
    val failedTasksAlpha =
      if (totalTasks > 0) {
        math.min(10 * failedTasks.toDouble / totalTasks, 1) * 0.5 + 0.5
      } else {
        1
      }
    // totalDuration range from 0 to 50% GC time, alpha max = 1
    val totalDurationAlpha =
      if (totalDuration > 0) {
        math.min(totalGCTime.toDouble / totalDuration + 0.5, 1)
      } else {
        1
      }

    val tableData =
      <td style={
          if (activeTasks > 0) {
            "background:hsla(240, 100%, 50%, " + activeTasksAlpha + ");color:white"
          } else {
            ""
          }
          }>{activeTasks}</td>
        <td style={
            if (failedTasks > 0) {
              "background:hsla(0, 100%, 50%, " + failedTasksAlpha + ");color:white"
            } else {
              ""
            }
            }>{failedTasks}</td>
        <td>{completedTasks}</td>
        <td>{totalTasks}</td>
        <td sorttable_customkey={totalDuration.toString} style={
        // Red if GC time over GCTimePercent of total time
        if (totalGCTime > GCTimePercent * totalDuration) {
          "background:hsla(0, 100%, 50%, " + totalDurationAlpha + ");color:white"
        } else {
          ""
        }
        }>
          {Utils.msDurationToString(totalDuration)}
          ({Utils.msDurationToString(totalGCTime)})
        </td>

    tableData
  }

}
