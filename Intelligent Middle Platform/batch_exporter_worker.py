# =================================================================
# 文件名: batch_exporter_worker.py
# 描述: 包含批量数据导出任务的 Worker 类，用于在后台执行。
# =================================================================

import requests
import json
import time
import os
import sys
from datetime import datetime
from typing import Dict, Any, List, Callable, Tuple
# 引入 QThread 以便在 worker 中使用 time.sleep 替代 QThread.msleep
from PySide6.QtCore import QObject, Signal, Slot, QSettings, QThread


# --- 1. 信号定义和配置 (已修正，移除 ExporterSignals 封装) ---

# 统一的 Cookie 获取函数名称
def get_cookie_data() -> Dict[str, str]:
    """
    尝试从主应用（SettingsPage）获取配置的 Cookie。
    如果导入失败（例如在独立测试或缺少主应用环境时），则回退到使用硬编码占位符。

    注意：我们不再实现复杂的 QSettings 回退，因为 SettingsPage.get_all_cookies
    已经封装了 QSettings 逻辑。如果导入 SettingsPage 失败，则假定环境不完整。
    """
    # 优先尝试从主文件导入 SettingsPage
    try:
        # ⚠️ 确保这里的导入路径与您的主文件结构一致
        # 如果 SettingsPage 在主文件 (main.py) 中，使用 from __main__ 导入
        # 如果 SettingsPage 在单独模块 (settings.py) 中，使用 from your_module import SettingsPage
        from __main__ import SettingsPage

        # 成功导入：使用 SettingsPage 的实际静态方法
        return SettingsPage.get_all_cookies()

    except (ImportError, AttributeError):
        # 导入失败：回退到使用硬编码占位符
        print("Warning: SettingsPage not found or inaccessible. Using hardcoded COOKIES_DICT as placeholder.")
        return {
            "AEOLUS_MOZI_TOKEN": "PLACEHOLDER_TOKEN",
            "xlly_s": "1",
            "PASSPORT_TOKEN": "null",
            # 仅包含最重要的几个键
        }

# 通用 Headers (保持不变)
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept': 'application/json, text/plain, */*',
    'Content-Type': 'application/json',
    'x-shard': 'shardid=1'
}


# --- 2. 批量导出工作线程 (BatchExporterWorker) ---

class BatchExporterWorker(QObject):
    """
    负责执行多项数据导出任务的 Worker。
    此版本已修正信号结构，并适配 run_single / run_batch 模式，并包含失败追踪。
    """

    finished_single = Signal(str, str, str)  # (task_key, status, file_name)
    finished_batch = Signal(str, str)  # (status: "成功"/"失败"/"取消", output_dir)
    progress_update = Signal(int, str)  # (当前任务索引, 状态信息)
    error_occurred = Signal(str)  # (错误消息)

    def __init__(self, output_dir: str, parent=None):
        super().__init__(parent)

        self.is_running = True
        self.has_batch_failed = False  # ✅ 新增：跟踪批量任务中是否发生错误
        self.output_dir = output_dir

        self.team_ids: List[int] = []
        self.date_params: Dict[str, Dict[str, Any]] = {}
        self._last_file_name = ""  # 用于单任务模式下返回文件名

        os.makedirs(self.output_dir, exist_ok=True)

        # 定义任务列表： (task_key, job_name, func)
        self.tasks: List[Tuple[str, str, Callable]] = [
            ("violation", "风神违规数据", self._task_violation),
            ("schedule", "骑手排班数据", self._task_schedule),
            ("attendance", "骑手考勤数据", self._task_attendance),
            ("daily_detail", "骑手每日详情数据", self._task_daily_detail)
        ]
        self.total_tasks = len(self.tasks)
        # 映射 task_key 到 (job_name, func)，方便单任务模式调用
        self.task_key_map = {task[0]: (task[1], task[2]) for task in self.tasks}

    def set_export_parameters(self, team_ids: List[int], date_params: Dict[str, Dict[str, str]], task_keys: List[str]):
        """
        设置所有导出任务所需的参数。
        """
        self.team_ids = team_ids
        self.date_params = date_params
        self._last_file_name = ""

    def stop(self):
        """外部调用以安全停止 Worker"""
        self.is_running = False

    # =================================================================
    # 单任务模式入口
    # =================================================================
    @Slot(str)
    def run_single(self, task_key: str):
        """执行单个导出任务。"""
        if not self.is_running:
            self.finished_single.emit(task_key, "取消", "")
            return

        cookies_dict = get_cookie_data()
        if not cookies_dict.get(list(cookies_dict.keys())[0]):
            self.error_occurred.emit("配置错误：核心 Cookie 值缺失或为占位符，无法执行任务。")
            self.finished_single.emit(task_key, "失败", "")
            return

        task_info = self.task_key_map.get(task_key)
        if not task_info:
            self.error_occurred.emit(f"任务键名错误：找不到任务 {task_key} 对应的处理函数。")
            self.finished_single.emit(task_key, "失败", "")
            return

        job_name, task_func = task_info

        try:
            # 1. 发送进度提示
            self.progress_update.emit(1, f"⚙️ 正在执行单任务: {job_name}...")

            # 2. 执行具体任务函数
            task_func(cookies_dict)  # 执行业务逻辑，结果文件名会被存在 self._last_file_name

            # 3. 检查任务是否成功完成
            if self.is_running and self._last_file_name:
                self.finished_single.emit(task_key, "成功", self._last_file_name)
            elif not self.is_running:
                self.finished_single.emit(task_key, "取消", "")
            else:
                # 注意: 如果执行到这里，说明任务函数内部调用 _execute_export_job 时发生了错误并发送了 error_occurred 信号
                self.finished_single.emit(task_key, "失败", "")

        except Exception as e:
            self.error_occurred.emit(f"任务 {job_name} 运行中发生致命错误: {e.__class__.__name__}: {e}")
            self.finished_single.emit(task_key, "失败", "")

    # =================================================================
    # 批量任务模式入口
    # =================================================================
    @Slot()
    def run_batch(self):
        """线程主执行函数，执行所有任务。"""

        if not self.team_ids or not self.date_params:
            self.error_occurred.emit("参数错误：请先设置团队ID和日期参数。")
            self.finished_batch.emit("失败", self.output_dir)
            return

        cookies_dict = get_cookie_data()

        try:
            if not cookies_dict.get(list(cookies_dict.keys())[0]):
                self.error_occurred.emit("配置错误：核心 Cookie 值缺失，无法执行任务。")
                self.finished_batch.emit("失败", self.output_dir)
                return

            self.has_batch_failed = False  # 开始批量任务前重置失败标志

            for i, (task_key, job_name, task_func) in enumerate(self.tasks):
                current_index = i + 1

                if not self.is_running:
                    self.progress_update.emit(current_index, f"任务已手动停止，停止在: {job_name}")
                    break

                self.progress_update.emit(current_index, f"⚙️ 正在执行任务: {job_name}...")

                # 执行具体任务函数
                task_func(cookies_dict)

                # 使用 QThread.msleep 代替 time.sleep 以避免阻塞 Qt 事件循环
                QThread.msleep(100)

            # ✅ 修正：根据失败标志判断最终状态
            if not self.is_running:
                self.finished_batch.emit("取消", self.output_dir)
            elif self.has_batch_failed:
                self.finished_batch.emit("失败", self.output_dir)
            else:
                self.finished_batch.emit("成功", self.output_dir)

        except Exception as e:
            self.error_occurred.emit(f"程序运行中发生致命错误: {e.__class__.__name__}: {e}")
            self.finished_batch.emit("失败", self.output_dir)

    # -------------------------------------------------------------------
    # 核心辅助方法：下载文件 (_download_file_from_url)
    # -------------------------------------------------------------------
    def _download_file_from_url(self, file_url: str, job_name: str, filename: str) -> bool:
        """接收一个下载链接，发起GET请求并保存文件到本地。"""
        output_path = os.path.join(self.output_dir, filename)

        try:
            download_response = requests.get(
                file_url,
                stream=True,
                timeout=30
            )
            download_response.raise_for_status()

            with open(output_path, 'wb') as f:
                for chunk in download_response.iter_content(chunk_size=8192):
                    if not self.is_running:
                        if os.path.exists(output_path):
                            os.remove(output_path)
                        return False
                    if chunk:
                        f.write(chunk)

            # ** 诊断点 4：文件大小检查 **
            file_size = os.path.getsize(output_path)
            if file_size < 1024 and file_size > 0:  # 小于 1KB 且大于 0，可能是错误提示文件
                print(f"[{job_name}] DEBUG: 文件大小异常 ({file_size} bytes)。")
                # 抛出异常，让它被下方的 except 捕获
                raise Exception(f"文件大小异常 ({file_size} bytes)，可能下载失败或内容为空。")
            elif file_size == 0:
                # 0 字节文件是明确的下载失败
                raise Exception(f"文件大小为 0 字节。")

            print(f"[{job_name}] DEBUG: 文件成功写入，大小: {file_size} bytes, 路径: {output_path}")

            self._last_file_name = filename
            self.progress_update.emit(self.total_tasks, f"✅ {job_name} 下载成功：{filename}")
            return True

        except requests.exceptions.RequestException as e:
            error_msg = f"❌ {job_name} 文件下载失败 (下载链接): {e}"
            self.error_occurred.emit(error_msg)
            self.has_batch_failed = True
            if os.path.exists(output_path):
                os.remove(output_path)  # 清理失败的文件
            return False

        except Exception as e:
            # 捕获文件大小异常
            error_msg = f"❌ {job_name} 文件写入校验失败: {e}"
            self.error_occurred.emit(error_msg)
            self.has_batch_failed = True
            if os.path.exists(output_path):
                os.remove(output_path)  # 清理失败的文件
            return False

    # -------------------------------------------------------------------
    # 核心导出执行器 (_execute_export_job)
    # -------------------------------------------------------------------
    def _execute_export_job(
            self,
            job_name: str,
            url: str,
            payload: Dict[str, Any],
            filename_prefix: str,
            cookies_dict: Dict[str, str],
            is_direct_download: bool = False,
            start_date: str = "",  # 新增
            end_date: str = ""     # 新增
    ):
        """执行通用的导出请求，文件名使用传入的日期范围"""
        if not self.is_running:
            return

        self._last_file_name = ""
        headers = HEADERS.copy()

        # --- 构造日期后缀 ---
        # 如果传入了日期，格式为: 20231201_20231220；否则回退到当前时间
        if start_date and end_date:
            date_suffix = f"{start_date.replace('-', '')}_{end_date.replace('-', '')}"
        else:
            date_suffix = datetime.now().strftime('%Y%m%d_%H%M%S')

        try:
            # --- 情况 A: 直接返回文件流 ---
            if is_direct_download:
                response = requests.post(url, headers=headers, json=payload, stream=True, timeout=30, cookies=cookies_dict)
                response.raise_for_status()

                content_type = response.headers.get('Content-Type', '').lower()
                ext = ".csv" if "csv" in content_type else ".xlsx"

                # 使用日期范围命名
                filename = f"{filename_prefix}_{date_suffix}{ext}"
                output_path = os.path.join(self.output_dir, filename)

                with open(output_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if not self.is_running:
                            if os.path.exists(output_path): os.remove(output_path)
                            return
                        if chunk: f.write(chunk)

                self._last_file_name = filename
                self.progress_update.emit(self.total_tasks, f"✅ {job_name} 下载成功：{filename}")
                return

            # --- 情况 B: 返回 JSON 包含下载链接 ---
            response = requests.post(url, headers=headers, json=payload, timeout=30, cookies=cookies_dict)
            response.raise_for_status()
            result = response.json()

            download_url = None
            if result.get('code') == 200:
                data = result.get('result', {}).get('data', {}) or result.get('data', {})
                download_url = data.get('fileUrl') or data.get('url')

            if download_url:
                ext = ".csv" if ".csv" in download_url.lower() else ".xlsx"
                # 使用日期范围命名
                filename = f"{filename_prefix}_{date_suffix}{ext}"

                if not self._download_file_from_url(download_url, job_name, filename):
                    self._last_file_name = ""
                    return
            else:
                self.error_occurred.emit(f"❌ {job_name} 接口未返回下载链接。")
                self.has_batch_failed = True

        except Exception as e:
            self.error_occurred.emit(f"❌ {job_name} 运行异常: {str(e)}")
            self.has_batch_failed = True

    # -------------------------------------------------------------------
    # 任务定义函数 (保持业务逻辑不变)
    # -------------------------------------------------------------------

    def _task_violation(self, cookies_dict: Dict[str, str]):
        """执行风神违规数据导出任务。"""
        start_date = self.date_params.get("violation", {}).get("start", "")
        end_date = self.date_params.get("violation", {}).get("end", "")

        start_ts_ms = int(datetime.strptime(start_date, "%Y-%m-%d").timestamp() * 1000)
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        end_ts_ms = int((end_dt.timestamp() + 86399) * 1000)  # 结束时间到当日 23:59:59

        payload = {
            "params": {
                "request": {
                    "violationStartTime": start_ts_ms,
                    "violationEndTime": end_ts_ms,
                    "appealStatus": []
                }
            }
        }
        self._execute_export_job(
            job_name="风神违规数据",
            url="https://httpizza.ele.me/xtop/xtop.lpd.quality.control.violation.violationOrderAeolusCenterApi.download/1.0",
            payload=payload,
            filename_prefix="风神服务奖惩数据",
            cookies_dict=cookies_dict,
            start_date=start_date,  # 传入开始日期
            end_date=end_date  # 传入结束日期
        )

    def _task_schedule(self, cookies_dict: Dict[str, str]):
        """执行骑手排班数据导出任务。"""
        start_date = self.date_params.get("schedule", {}).get("start", "")
        end_date = self.date_params.get("schedule", {}).get("end", "")

        payload = {
            "params": {
                "request": {
                    "teamIds": self.team_ids,
                    "endDate": end_date,
                    "startDate": start_date
                }
            }
        }
        self._execute_export_job(
            job_name="骑手排班数据",
            url="https://httpizza.ele.me/xtop/xtop.fs.special.schedule.exportSchedule/1.0",
            payload=payload,
            filename_prefix="骑手排班信息",
            cookies_dict=cookies_dict,
            start_date=start_date,  # 传入开始日期
            end_date=end_date  # 传入结束日期
        )

    def _task_attendance(self, cookies_dict: Dict[str, str]):
        """执行骑手考勤数据导出任务。"""
        start_date = self.date_params.get("attendance", {}).get("start", "")
        end_date = self.date_params.get("attendance", {}).get("end", "")

        payload = {
            "params": {
                "request": {
                    "teamIds": self.team_ids,
                    "endDate": end_date,
                    "startDate": start_date
                }
            }
        }
        self._execute_export_job(
            job_name="骑手考勤数据",
            url="https://httpizza.ele.me/xtop/xtop.fs.special.examine.exportStatisticsDetail/1.0",
            payload=payload,
            filename_prefix="骑手每日考勤明细",
            cookies_dict=cookies_dict,
            start_date=start_date,  # 传入开始日期
            end_date=end_date  # 传入结束日期
        )

    def _task_daily_detail(self, cookies_dict: Dict[str, str]):
        """执行骑手每日详情数据导出任务（直接下载）。"""
        start_date = self.date_params.get("daily_detail", {}).get("start", "")
        end_date = self.date_params.get("daily_detail", {}).get("end", "")

        payload = {
            "teamIds": self.team_ids,
            "endAt": end_date,
            "startAt": start_date,
            "exportType": 2,
            "fields": [
                # 保持您提供的完整字段列表
                "squadName", "levelStageName", "takingWorkAt", "dimissionAt", "isInservice",
                "takingWorkDays", "isActiveRider", "carrierDriverValidOnlineTime", "noonCarrierDriverValidOnlineTime",
                "nightCarrierDriverValidOnlineTime", "carrierDriverValidOnlineTime", "carrierDriverRestDuration",
                "carrierDriverRestDurationAvg",
                "systemAcceptOrderCount", "validCompleteCount", "changeOrderCount", "changeOrderAvg",
                "robOrderCount", "scheduleTimeAvg", "arriveShopTimeAvg", "pickupTimeAvg", "deliverTimeAvg",
                "tmsDurationAvg", "deliveryDurationAvg", "deliveryDistanceAvg", "feedbackCount", "overRidertCount",
                "overRidertRate", "customerTOvertimeCount", "customerTOvertimeRate", "customerT8OvertimeCount",
                "customerT8OvertimeRate", "customerT20OvertimeCount", "customerT20OvertimeRate",
                "cancelOrderCountForKnight",
                "cancelOrderRateForKnight", "carrierCancelOrderCount", "carrierCancelOrderRate", "defraudCount",
                "defraudRate", "arrivedDefraudCountForKnight", "arrivedDefraudRateForKnight",
                "pickupDefraudCountForKnight",
                "pickupDefraudRateForKnight", "deliveryDefraudCountForKnight", "deliveryDefraudRateForKnight",
                "establishedComplainCount",
                "establishedComplainRate", "establishedCustomerComplainCount", "establishedMerchantComplainCount",
                "shopClaimCount",
                "establishedBadEvaluateCount", "establishedBadEvaluateRate", "goodEvaluateCountForKnight",
                "goodEvaluateRateForKnight",
                "prodQcRiderCount", "failedQcRiderCount", "fraudCount"
            ]
        }
        self._execute_export_job(
            job_name="骑手每日详情数据",
            url="https://httpizza.ele.me/lpd_soc.dashboard/apollo/carrier_driver/analysis/pizza/agency/export",
            payload=payload,
            filename_prefix="骑手每日详情数据",
            cookies_dict=cookies_dict,
            is_direct_download=True,
            start_date=start_date,  # 传入开始日期
            end_date=end_date  # 传入结束日期
        )