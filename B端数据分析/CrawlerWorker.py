import threading
import requests
import json
import time
import csv
import os
from datetime import datetime
from typing import List, Dict, Any
from __main__ import SettingsPage
from PySide6.QtCore import Signal, QObject, QSettings  # 确保从 PySide6.QtCore 导入


# --- CrawlerWorker 信号托管类 ---
# 由于 threading.Thread 不继承自 QObject，我们需要一个 QObject 子类来托管信号。
class SignalHost(QObject):
    success_signal = Signal(str, int)  # (文件绝对路径, 记录总数)
    error_signal = Signal(str)  # (错误消息)
    progress_signal = Signal(int, int)  # (当前页, 总页数)



try:
    from __main__ import SettingsPage
except ImportError:
    print("Warning: SettingsPage not found. Using placeholder SettingsPage.")


    class SettingsPage:
        REQUIRED_COOKIES = ["AEOLUS_MOZI_TOKEN", "xlly_s", "PASSPORT_TOKEN", "PASSPORT_AGENTS_TOKEN", "cna", "isg"]
        SETTINGS_GROUP = "CrawlerSettings"

        @staticmethod
        def get_all_cookies() -> Dict[str, str]:
            # 返回一个模拟的、缺失重要 Token 的 Cookie 字典，用于触发错误
            return {"AEOLUS_MOZI_TOKEN": "", "xlly_s": "1", "cna": "test"}


# --- 爬虫工作线程 (CrawlerWorker) ---

class CrawlerWorker(threading.Thread):
    """
    爬虫工作线程，用于在后台执行耗时的网络请求和数据处理。
    依赖 SettingsPage 来获取配置的 Cookie。
    """

    def __init__(self, output_filename: str):
        super().__init__()

        # 信号托管实例
        self.signals = SignalHost()

        # --- 配置 ---
        self.base_url = "https://httpizza.ele.me/lpd.meepo.mgmt/knight/queryKnightDimissionRecords"
        self.page_size = 50
        self.output_filename = output_filename

        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Referer': 'https://aeolus.ele.me/'
        }

    def convert_to_formatted_data(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """将数据中的毫秒时间戳转换为可读的日期时间字符串。"""
        formatted_records = []
        for record in records:
            # 复制字典
            new_record = record.copy()

            # 转换 unBindTime (离职时间)
            unBindTime_ms = new_record.get('unBindTime')
            if unBindTime_ms and isinstance(unBindTime_ms, (int, float)):
                timestamp_sec = unBindTime_ms / 1000
                new_record['unBindTime_formatted'] = datetime.fromtimestamp(timestamp_sec).strftime('%Y-%m-%d %H:%M:%S')
                del new_record['unBindTime']
            else:
                new_record['unBindTime_formatted'] = ''

            formatted_records.append(new_record)
        return formatted_records

    def write_to_csv(self, data: List[Dict[str, Any]], filename: str):
        """将字典列表写入 CSV 文件。"""
        if not data:
            return

        # 确定字段名
        fieldnames = list(data[0].keys())

        # 使用 encoding='utf-8-sig' 确保 Excel 正确识别中文 (带 BOM)
        with open(filename, 'w', newline='', encoding='utf-8-sig') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)

    def run(self):
        """线程执行的主逻辑 (爬虫主体)"""

        # 1. 动态获取 Cookie 值
        cookies_dict = SettingsPage.get_all_cookies()

        # 检查最关键的 Token 是否存在
        main_token = SettingsPage.REQUIRED_COOKIES[0]
        if not cookies_dict.get(main_token):
            self.signals.error_signal.emit(f"配置错误：请先在【设置】页面配置并保存关键 Cookie ({main_token} 不能为空)。")
            return

        all_records = []
        current_page = 1
        total_records = 0
        total_pages = 1

        try:
            while current_page <= total_pages:
                params = {
                    'pageIndex': current_page,
                    'pageSize': self.page_size
                }

                # 发送请求时使用动态获取的 cookies_dict
                response = requests.get(
                    self.base_url,
                    headers=self.headers,
                    cookies=cookies_dict,
                    params=params,
                    timeout=10
                )

                if response.status_code != 200:
                    raise Exception(f"请求失败，状态码: {response.status_code}")

                data = response.json()

                if data.get('code') != '200':
                    msg = data.get('msg', '未知错误')
                    raise Exception(f"API 返回错误码: {data.get('code')}，消息: {msg}")

                data_body = data.get('data', {})
                record_list = data_body.get('data', [])

                if current_page == 1:
                    total_records = data_body.get('total', 0)
                    if total_records > 0:
                        total_pages = (total_records + self.page_size - 1) // self.page_size
                    else:
                        break  # 无记录，退出循环

                if not record_list:
                    break

                all_records.extend(record_list)

                # 发送进度信号
                self.signals.progress_signal.emit(current_page, total_pages)

                current_page += 1

            # --- 数据处理与导出 ---
            total_count = len(all_records)
            if total_count > 0:
                formatted_data = self.convert_to_formatted_data(all_records)
                self.write_to_csv(formatted_data, self.output_filename)

                # 发送成功信号
                self.signals.success_signal.emit(os.path.abspath(self.output_filename), total_count)
            else:
                # 成功完成但没有数据
                self.signals.success_signal.emit(self.output_filename, 0)

        except requests.exceptions.Timeout:
            self.signals.error_signal.emit("请求超时，请检查网络连接或 Cookie 有效性。")
        except requests.exceptions.RequestException as e:
            self.signals.error_signal.emit(f"网络请求错误: {e}")
        except json.JSONDecodeError:
            self.signals.error_signal.emit("解析服务器返回的 JSON 失败。")
        except Exception as e:
            self.signals.error_signal.emit(f"发生未知错误: {e}")