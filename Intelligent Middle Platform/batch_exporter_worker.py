# =================================================================
# 文件名: batch_exporter_worker.py
# 描述: 纯净后台 Worker，包含完整字段列表，彻底解决界面卡死问题。
# =================================================================

import requests
import json
import os
from datetime import datetime
from typing import Dict, Any, List, Callable, Tuple
from PySide6.QtCore import QObject, Signal, Slot, QThread
import os
# 重点：这行代码能解决 90% 以上由全局代理引起的 Python 网络卡顿
os.environ['NO_PROXY'] = 'ele.me,aliyuncs.com,bdimg.com'

# 通用 Headers
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept': 'application/json, text/plain, */*',
    'Content-Type': 'application/json',
    'x-shard': 'shardid=1'
}


class BatchExporterWorker(QObject):
    """
    负责执行数据导出任务的后台 Worker。
    参数通过 set_export_parameters 注入，避免子线程直接访问 UI 导致死锁。
    """

    finished_single = Signal(str, str, str)  # (task_key, status, file_name)
    finished_batch = Signal(str, str)  # (status, output_dir)
    progress_update = Signal(int, str)  # (index, message)
    error_occurred = Signal(str)  # (error_message)

    def __init__(self, output_dir: str, parent=None):
        super().__init__(parent)
        self.is_running = True
        self.has_batch_failed = False
        self.output_dir = output_dir

        self.team_ids: List[int] = []
        self.date_params: Dict[str, Dict[str, Any]] = {}
        self.cookies: Dict[str, str] = {}  # 由 UI 主线程传入
        self._last_file_name = ""

        os.makedirs(self.output_dir, exist_ok=True)

        # 任务映射关系
        self.tasks_info = {
            "violation": ("风神违规数据", self._task_violation),
            "schedule": ("骑手排班数据", self._task_schedule),
            "attendance": ("骑手考勤数据", self._task_attendance),
            "daily_detail": ("骑手每日详情数据", self._task_daily_detail)
        }

    def set_export_parameters(self, team_ids: List[int], date_params: Dict[str, Dict[str, str]],
                              cookies: Dict[str, str]):
        """注入必要参数，不再依赖从主线程实时抓取"""
        self.team_ids = team_ids
        self.date_params = date_params
        self.cookies = cookies
        self._last_file_name = ""

    def stop(self):
        self.is_running = False

    # =================================================================
    # 任务执行入口
    # =================================================================

    @Slot(str)
    def run_single(self, task_key: str):
        """单任务模式入口"""
        if not self.is_running: return

        if not self.cookies or not any(self.cookies.values()):
            self.error_occurred.emit("未检测到有效 Cookie，请在设置页检查配置。")
            self.finished_single.emit(task_key, "失败", "")
            return

        info = self.tasks_info.get(task_key)
        if not info:
            self.error_occurred.emit(f"未定义的任务类型: {task_key}")
            return

        job_name, task_func = info
        try:
            self.progress_update.emit(1, f"⚙️ 正在导出: {job_name}...")
            task_func()

            if self.is_running and self._last_file_name:
                self.finished_single.emit(task_key, "成功", self._last_file_name)
            else:
                self.finished_single.emit(task_key, "失败", "")
        except Exception as e:
            self.error_occurred.emit(f"{job_name} 运行中发生错误: {str(e)}")
            self.finished_single.emit(task_key, "失败", "")

    @Slot()
    def run_batch(self):
        """全量批量导出模式入口"""
        if not self.team_ids or not self.cookies:
            self.error_occurred.emit("参数不完整，请检查团队ID和Cookie设置。")
            self.finished_batch.emit("失败", self.output_dir)
            return

        self.has_batch_failed = False
        keys = list(self.tasks_info.keys())

        for i, task_key in enumerate(keys):
            if not self.is_running: break

            job_name, task_func = self.tasks_info[task_key]
            self.progress_update.emit(i + 1, f"⚙️ 批量进度 ({i + 1}/{len(keys)}): {job_name}")

            try:
                task_func()
            except Exception as e:
                self.error_occurred.emit(f"任务 {job_name} 异常: {str(e)}")
                self.has_batch_failed = True

        status = "取消" if not self.is_running else ("失败" if self.has_batch_failed else "成功")
        self.finished_batch.emit(status, self.output_dir)

    # -------------------------------------------------------------------
    # 核心私有辅助方法
    # -------------------------------------------------------------------

    def _execute_export_job(self, job_name: str, url: str, payload: Dict[str, Any],
                            filename_prefix: str, is_direct_download: bool = False,
                            task_key: str = ""):
        """通用核心导出逻辑"""
        if not self.is_running: return
        self._last_file_name = ""

        # 构造统一的文件名后缀
        dates = self.date_params.get(task_key, {})
        start_str = dates.get("start", "").replace("-", "")
        end_str = dates.get("end", "").replace("-", "")
        date_suffix = f"{start_str}_{end_str}" if start_str else datetime.now().strftime("%Y%m%d")

        try:
            if is_direct_download:
                # 处理直接返回流的情况
                resp = requests.post(url, headers=HEADERS, json=payload, stream=True, timeout=30, cookies=self.cookies)
                resp.raise_for_status()
                filename = f"{filename_prefix}_{date_suffix}.xlsx"
                if self._write_to_disk(resp, filename):
                    self._last_file_name = filename
                return

            # 处理返回 JSON 下载链接的情况
            resp = requests.post(url, headers=HEADERS, json=payload, timeout=30, cookies=self.cookies)
            resp.raise_for_status()
            data = resp.json()

            res_data = data.get('result', {}).get('data', {}) or data.get('data', {})
            file_url = res_data.get('fileUrl') or res_data.get('url')

            if file_url:
                filename = f"{filename_prefix}_{date_suffix}.xlsx"
                if self._download_file(file_url, filename):
                    self._last_file_name = filename
            else:
                raise Exception("服务端未返回下载链接 (URL is Empty)")

        except Exception as e:
            self.error_occurred.emit(f"[{job_name}] 任务失败: {str(e)}")
            self.has_batch_failed = True

    def _write_to_disk(self, response, filename) -> bool:
        """分块写入文件，确保大文件下载时不阻塞"""
        path = os.path.join(self.output_dir, filename)
        try:
            with open(path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=16384):  # 16KB 块大小
                    if not self.is_running: return False
                    if chunk: f.write(chunk)
            return True
        except Exception as e:
            self.error_occurred.emit(f"写入磁盘出错: {str(e)}")
            return False

    def _download_file(self, url, filename) -> bool:
        """从文件服务器地址下载"""
        try:
            r = requests.get(url, stream=True, timeout=60)
            r.raise_for_status()
            return self._write_to_disk(r, filename)
        except Exception as e:
            self.error_occurred.emit(f"下载文件流失败: {str(e)}")
            return False

    # -------------------------------------------------------------------
    # 各任务具体实现函数 (包含完整字段)
    # -------------------------------------------------------------------

    def _task_violation(self):
        """1. 风神违规数据"""
        dates = self.date_params.get("violation", {})
        start_ts = int(datetime.strptime(dates['start'], "%Y-%m-%d").timestamp() * 1000)
        end_ts = int((datetime.strptime(dates['end'], "%Y-%m-%d").timestamp() + 86399) * 1000)

        payload = {
            "params": {"request": {"violationStartTime": start_ts, "violationEndTime": end_ts, "appealStatus": []}}}
        self._execute_export_job("风神违规",
                                 "https://httpizza.ele.me/xtop/xtop.lpd.quality.control.violation.violationOrderAeolusCenterApi.download/1.0",
                                 payload, "风神服务奖惩数据", False, "violation")

    def _task_schedule(self):
        """2. 骑手排班数据"""
        dates = self.date_params.get("schedule", {})
        payload = {
            "params": {"request": {"teamIds": self.team_ids, "startDate": dates['start'], "endDate": dates['end']}}}
        self._execute_export_job("排班数据", "https://httpizza.ele.me/xtop/xtop.fs.special.schedule.exportSchedule/1.0",
                                 payload, "骑手排班信息", False, "schedule")

    def _task_attendance(self):
        """3. 骑手考勤数据"""
        dates = self.date_params.get("attendance", {})
        payload = {
            "params": {"request": {"teamIds": self.team_ids, "startDate": dates['start'], "endDate": dates['end']}}}
        self._execute_export_job("考勤数据",
                                 "https://httpizza.ele.me/xtop/xtop.fs.special.examine.exportStatisticsDetail/1.0",
                                 payload, "骑手每日考勤明细", False, "attendance")

    def _task_daily_detail(self):
        """4. 骑手每日详情数据 - 包含 57 个完整字段"""
        dates = self.date_params.get("daily_detail", {})
        payload = {
            "teamIds": self.team_ids,
            "endAt": dates['end'],
            "startAt": dates['start'],
            "exportType": 2,
            "fields": [
                "squadName", "levelStageName", "takingWorkAt", "dimissionAt", "isInservice",
                "takingWorkDays", "isActiveRider", "carrierDriverValidOnlineTime", "noonCarrierDriverValidOnlineTime",
                "nightCarrierDriverValidOnlineTime", "carrierDriverRestDuration", "carrierDriverRestDurationAvg",
                "systemAcceptOrderCount", "validCompleteCount", "changeOrderCount", "changeOrderAvg",
                "robOrderCount", "scheduleTimeAvg", "arriveShopTimeAvg", "pickupTimeAvg", "deliverTimeAvg",
                "tmsDurationAvg", "deliveryDurationAvg", "deliveryDistanceAvg", "feedbackCount", "overRidertCount",
                "overRidertRate", "customerTOvertimeCount", "customerTOvertimeRate", "customerT8OvertimeCount",
                "customerT8OvertimeRate", "customerT20OvertimeCount", "customerT20OvertimeRate",
                "cancelOrderCountForKnight", "cancelOrderRateForKnight", "carrierCancelOrderCount",
                "carrierCancelOrderRate",
                "defraudCount", "defraudRate", "arrivedDefraudCountForKnight", "arrivedDefraudRateForKnight",
                "pickupDefraudCountForKnight", "pickupDefraudRateForKnight", "deliveryDefraudCountForKnight",
                "deliveryDefraudRateForKnight", "establishedComplainCount", "establishedComplainRate",
                "establishedCustomerComplainCount", "establishedMerchantComplainCount", "shopClaimCount",
                "establishedBadEvaluateCount", "establishedBadEvaluateRate", "goodEvaluateCountForKnight",
                "goodEvaluateRateForKnight", "prodQcRiderCount", "failedQcRiderCount", "fraudCount"
            ]
        }
        self._execute_export_job("每日详情",
                                 "https://httpizza.ele.me/lpd_soc.dashboard/apollo/carrier_driver/analysis/pizza/agency/export",
                                 payload, "骑手每日详情数据", True, "daily_detail")