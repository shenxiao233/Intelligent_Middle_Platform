import sys
import pandas as pd
from datetime import datetime
from typing import Dict
import qtawesome as qta
import threading
import requests
from PySide6.QtWidgets import (
    QApplication, QMainWindow,QFrame,QGraphicsDropShadowEffect, QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QLineEdit, QGridLayout,QPushButton, QMessageBox, QFileDialog, QSizePolicy,
    QStackedWidget, QGraphicsOpacityEffect,QSystemTrayIcon,QMenu,
    QProgressBar,QStatusBar
)
from PySide6.QtCore import QPropertyAnimation, QEasingCurve, QSize
from PySide6.QtCore import QSettings, QEvent,QTimer
from PySide6.QtWidgets import QTextEdit # 新增导入
import json # 新增导入
from PySide6.QtWidgets import QScrollArea # 确保在您的导入列表中
from PySide6.QtCore import Qt, Signal, QObject, Slot,QThreadPool
from PySide6.QtGui import QFont, QColor,QFontMetrics
import os
from data_worker import DataWorker
from xlsx_to_csv_page import XlsxToCsvPage
from Export_data_page import BatchExportPage

# --- 设置页面 (SettingsPage) ---
class SettingsPage(QWidget):
    PAGE_NAME = "设置"
    # 静态配置的 Cookie 字段名列表
    REQUIRED_COOKIES = [
        "AEOLUS_MOZI_TOKEN",
        "xlly_s",
        "PASSPORT_TOKEN",
        "PASSPORT_AGENTS_TOKEN",
        "cna",
        "isg",
        # 如果还有其他关键 Cookie 也可以加在这里
    ]

    SETTINGS_GROUP = "CrawlerSettings"

    def __init__(self, parent=None):
        super().__init__(parent)
        # QSettings 初始化需要 QApplication 的组织名和应用名
        QApplication.setOrganizationName("YourCompanyName")
        QApplication.setApplicationName("AutomationToolbox")
        # QSettings 会自动找到合适的配置文件路径 (Windows: 注册表, Linux/macOS: ini 文件)
        self.settings = QSettings()

        # 用于存储动态创建的输入框
        self.entry_fields: Dict[str, QLineEdit] = {}

        self._setup_ui()
        self.load_settings()
        self.apply_styles()

    def apply_styles(self):
        """应用现代化样式"""
        PRIMARY_COLOR = "#007AFF"
        SUCCESS_COLOR = "#34C759"
        PARSE_COLOR = "#E67E22"
        BACKGROUND_COLOR = "#F0F4F7"

        self.setStyleSheet(f"""
            QWidget {{ background-color: {BACKGROUND_COLOR}; }}

            QLabel {{ color: #2C3E50; font-family: "Microsoft YaHei"; font-size: 11pt; }}

            /* 输入框 */
            QLineEdit {{
                border: 1px solid #D1D5DA;
                border-radius: 6px;
                padding: 8px 10px;
                background-color: white;
                font-size: 10pt;
                min-height: 30px;
            }}
            QLineEdit:focus {{
                border: 1px solid {PRIMARY_COLOR};
            }}

            /* 文本编辑框 (用于粘贴 JSON) */
            QTextEdit {{
                border: 1px solid #D1D5DA;
                border-radius: 6px;
                padding: 8px;
                background-color: white;
                font-size: 10pt;
            }}

            /* 解析按钮 */
            #ParseButton {{
                background-color: {PARSE_COLOR};
                color: white;
                border-radius: 8px;
                padding: 8px 10px;
                font-weight: bold;
            }}
            #ParseButton:hover {{
                background-color: #D35400;
            }}

            /* 保存按钮 */
            #SaveButton {{
                background-color: {SUCCESS_COLOR};
                color: white;
                border-radius: 10px;
                padding: 12px;
                font-weight: bold;
                transition: background-color 0.2s;
            }}
            #SaveButton:hover {{
                background-color: #2CAE4E;
            }}
        """)

    def _setup_ui(self):
        # 1. 创建 SettingsPage 的主布局 (用于放置 QScrollArea)
        main_page_layout = QVBoxLayout(self)
        main_page_layout.setContentsMargins(0, 0, 0, 0)  # 滚动区域的边距通常设为0

        # 2. 创建一个内容容器 Widget，用于容纳所有表单元素
        content_widget = QWidget()

        # 3. 创建 ScrollArea，并设置 content_widget 为其内容
        scroll_area = QScrollArea()
        scroll_area.setWidgetResizable(True)
        scroll_area.setWidget(content_widget)
        scroll_area.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)

        # === 关键修改：应用美化后的样式 ===
        SCROLLBAR_QSS = """
                    QScrollBar:vertical {
                        border: none;
                        background: #F0F4F7; /* 背景颜色与页面背景匹配 */
                        width: 10px; 
                        margin: 0px 0px 0px 0px;
                    }

                    QScrollBar::handle:vertical {
                        background: #A0A0A0; /* 滑块颜色：较深的灰色 */
                        min-height: 20px;
                        border-radius: 5px; 
                    }

                    QScrollBar::handle:vertical:hover {
                        background: #808080; /* 悬停时颜色更深 */
                    }

                    QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {
                        border: none;
                        background: none;
                    }

                    QScrollBar::add-page:vertical, QScrollBar::sub-page:vertical {
                        background: none;
                    }
                """
        scroll_area.setStyleSheet(SCROLLBAR_QSS)

        # 4. 创建内部布局 (原有的 QVBoxLayout)
        layout = QVBoxLayout(content_widget)
        layout.setContentsMargins(50, 50, 50, 50)  # 设置内边距
        layout.setSpacing(25)

        # 标题 (原代码)
        title = QLabel("⚙️ 应用程序设置")
        title.setFont(QFont("Microsoft YaHei", 22, QFont.Bold))
        title.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(title)

        # ----------------------------------------------------
        # 1. JSON 粘贴区域 (原代码)
        # ----------------------------------------------------
        json_title = QLabel("步骤 1: 粘贴浏览器导出的 **完整 Cookie JSON 数据**")
        json_title.setStyleSheet("font-weight: bold; color: #E67E22;")
        layout.addWidget(json_title)

        self.entry_json = QTextEdit()
        self.entry_json.setPlaceholderText("请将浏览器插件导出的 JSON 数据粘贴到此处...")
        self.entry_json.setFixedHeight(150)
        layout.addWidget(self.entry_json)

        btn_parse = QPushButton("▶️ 解析并提取关键 Cookie")
        btn_parse.setObjectName("ParseButton")
        btn_parse.clicked.connect(self.parse_json_cookies)
        layout.addWidget(btn_parse)

        layout.addWidget(self.create_separator())

        # ----------------------------------------------------
        # 2. 关键 Cookie 显示与手动输入区域 (原代码 - 修正布局后的版本)
        # ----------------------------------------------------
        cookie_title = QLabel("步骤 2: 关键 Cookie 值 (确认后保存)")
        cookie_title.setStyleSheet("font-weight: bold; color: #007AFF;")
        layout.addWidget(cookie_title)

        form_layout = QVBoxLayout()
        form_layout.setSpacing(10)
        LABEL_WIDTH = 220

        for name in self.REQUIRED_COOKIES:
            hbox = QHBoxLayout()
            hbox.setContentsMargins(0, 0, 0, 0)
            hbox.setSpacing(10)

            label = QLabel(name + ":")
            label.setFixedWidth(LABEL_WIDTH)
            label.setStyleSheet("font-weight: 500;")

            entry = QLineEdit()
            entry.setPlaceholderText(f"将使用此 {name} 值进行爬取...")
            entry.setObjectName(f"Entry_{name}")
            entry.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)
            self.entry_fields[name] = entry

            hbox.addWidget(label)
            hbox.addWidget(entry)
            form_layout.addLayout(hbox)

        layout.addLayout(form_layout)

        # ----------------------------------------------------
        # 3. 保存按钮 (原代码)
        # ----------------------------------------------------
        btn_save = QPushButton("💾 保存配置")
        btn_save.setObjectName("SaveButton")
        btn_save.clicked.connect(self.save_settings)
        btn_save.setFixedHeight(50)
        layout.addWidget(btn_save)

        self.lbl_status = QLabel("")
        self.lbl_status.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.lbl_status.setStyleSheet("color: #7f8c8d;")
        layout.addWidget(self.lbl_status)

        layout.addStretch()  # 确保内容顶部对齐

        # 5. 将 ScrollArea 添加到 SettingsPage 的主布局
        main_page_layout.addWidget(scroll_area)

    def create_separator(self):
        line = QLabel()
        line.setFixedHeight(1)
        line.setStyleSheet("background-color: #D1D5DA; margin-top: 5px; margin-bottom: 5px;")
        return line

    def parse_json_cookies(self):
        """解析粘贴的 JSON 数组，提取关键 Cookie 值"""
        json_text = self.entry_json.toPlainText().strip()
        if not json_text:
            QMessageBox.warning(self, "解析失败", "请先将 JSON 数据粘贴到上方的文本框中。")
            return

        try:
            cookie_array = json.loads(json_text)
            if not isinstance(cookie_array, list):
                raise ValueError("JSON 格式不正确，应为 Cookie 对象数组。")

            extracted_values = {}
            found_count = 0

            for cookie in cookie_array:
                name = cookie.get("name")
                value = cookie.get("value")

                if name in self.REQUIRED_COOKIES and value:
                    extracted_values[name] = value
                    found_count += 1

            if not extracted_values:
                QMessageBox.critical(self, "解析失败",
                                     "未找到任何所需的关键 Cookie (如 AEOLUS_MOZI_TOKEN)。请检查粘贴的数据是否完整。")
                return

            # 将提取的值填充到输入框中
            for name, entry in self.entry_fields.items():
                entry.setText(extracted_values.get(name, ''))

            self.lbl_status.setText(f"✅ 成功解析 JSON，找到 {found_count} 个关键 Cookie。请点击保存。")
            self.lbl_status.setStyleSheet("color: #2ecc71; font-weight: bold;")

        except json.JSONDecodeError:
            QMessageBox.critical(self, "解析错误", "JSON 数据格式不正确，请确保粘贴了完整的、有效的 JSON 数组。")
        except ValueError as e:
            QMessageBox.critical(self, "数据错误", str(e))
        except Exception as e:
            QMessageBox.critical(self, "未知错误", f"解析时发生未知错误: {e}")

    def load_settings(self):
        """从 QSettings 加载保存的配置"""
        self.settings.beginGroup(self.SETTINGS_GROUP)
        found_count = 0
        for name, entry in self.entry_fields.items():
            # QSettings.value 返回的是字符串或 QVariant，在 Python 中通常自动转换为字符串
            cookie_value = self.settings.value(name, "")
            entry.setText(cookie_value)
            if cookie_value:
                found_count += 1
        self.settings.endGroup()

        if found_count > 0:
            self.lbl_status.setText(f"配置已加载，找到 {found_count} 个已保存的 Cookie。")
        else:
            self.lbl_status.setText("没有找到已保存的配置。")

    def save_settings(self):
        """保存配置到 QSettings"""
        # 简单检查主 Token 是否为空
        main_token_value = self.entry_fields.get(self.REQUIRED_COOKIES[0]).text().strip()
        if not main_token_value:
            QMessageBox.warning(self, "警告", f"主 Token ({self.REQUIRED_COOKIES[0]}) 不能为空。")
            return

        self.settings.beginGroup(self.SETTINGS_GROUP)

        saved_count = 0
        for name, entry in self.entry_fields.items():
            value = entry.text().strip()
            self.settings.setValue(name, value)
            if value:
                saved_count += 1

        self.settings.endGroup()

        QMessageBox.information(self, "成功", f"配置已成功保存！共保存 {saved_count} 个 Cookie 值。")
        self.lbl_status.setText("配置已保存。下次爬取将使用此值。")

    @staticmethod
    def get_all_cookies() -> Dict[str, str]:
        """静态方法：供外部（如 CrawlerWorker）调用以获取完整的 Cookie 字典"""
        # 必须重新实例化 QSettings，因为静态方法在任何地方都可能被调用
        settings = QSettings()
        settings.beginGroup(SettingsPage.SETTINGS_GROUP)

        all_cookies = {}
        # 遍历所有需要的键，从设置中读取
        for key in SettingsPage.REQUIRED_COOKIES:
            # 确保获取到的值是字符串类型
            value = str(settings.value(key, ""))
            if value:
                all_cookies[key] = value

        settings.endGroup()
        return all_cookies


# --- 1. 继承之前的拖拽输入框类 ---
class DropLineEdit(QLineEdit):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setAcceptDrops(True)

    def dragEnterEvent(self, event):
        if event.mimeData().hasUrls():
            event.acceptProposedAction()
        else:
            event.ignore()

    def dragMoveEvent(self, event):
        if event.mimeData().hasUrls():
            event.acceptProposedAction()
        else:
            event.ignore()

    def dropEvent(self, event):
        urls = event.mimeData().urls()
        if urls:
            file_path = os.path.normpath(urls[0].toLocalFile())
            self.setText(file_path)
            self.setFocus()


# --- 2. 页面 UI 类 ---
class WideTablePage(QWidget):
    PAGE_NAME = "宽表导出"
    DESC = "将离散字段聚合为业务侧所需的高性能宽表"

    DEFAULT_PREFIX_DAILY = '有效商户明细_FY26'
    DEFAULT_PREFIX_BAODAN = '爆单综合看板'

    def __init__(self, parent=None):
        super().__init__(parent)
        self.threadpool = QThreadPool()
        self._init_ui()
        self._apply_style()

    def _init_ui(self):
        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(30, 30, 30, 30)
        main_layout.setSpacing(20)

        # 头部标题
        header = QVBoxLayout()
        header.setSpacing(5)
        title_label = QLabel("宽表数据合并与指标计算")
        title_label.setObjectName("HeaderLabel")
        desc_label = QLabel("整合每日明细与爆单看板数据，自动计算关键业务指标。")
        desc_label.setObjectName("DescLabel")
        header.addWidget(title_label)
        header.addWidget(desc_label)
        main_layout.addLayout(header)

        # 配置中心卡片
        config_card = QFrame()
        config_card.setObjectName("ConfigCard")
        card_layout = QVBoxLayout(config_card)
        card_layout.setContentsMargins(25, 25, 25, 25)
        card_layout.setSpacing(18)

        # 1. 主表数据
        self.path_daily = self._create_input_section(
            card_layout,
            "1. 主表来源 (每日明细文件夹)",
            f"前缀过滤: {self.DEFAULT_PREFIX_DAILY}*.csv",
            self.select_daily_folder
        )

        # 2. 副表数据
        self.path_baodan = self._create_input_section(
            card_layout,
            "2. 副表来源 (爆单红包文件夹)",
            f"前缀过滤: {self.DEFAULT_PREFIX_BAODAN}*.csv",
            self.select_baodan_folder
        )

        # 3. 输出路径
        self.path_output = self._create_input_section(
            card_layout,
            "3. 结果输出路径",
            "请指定最终生成的 CSV 文件存放位置",
            self.select_output_file
        )

        main_layout.addWidget(config_card)

        # 底部操作区
        action_layout = QVBoxLayout()
        self.start_btn = QPushButton("🚀 启动数据处理与计算")
        self.start_btn.setObjectName("PrimaryBtn")
        self.start_btn.setFixedHeight(50)
        self.start_btn.clicked.connect(self.start_data_task)

        log_header = QLabel("任务实时日志")
        log_header.setObjectName("SectionTitle")

        self.log_output = QTextEdit()
        self.log_output.setObjectName("LogArea")
        self.log_output.setReadOnly(True)
        self.log_output.setPlaceholderText("等待任务启动...")

        action_layout.addWidget(self.start_btn)
        action_layout.addSpacing(10)
        action_layout.addWidget(log_header)
        action_layout.addWidget(self.log_output)

        main_layout.addLayout(action_layout)

    def _create_input_section(self, parent_layout, title, hint, callback):
        """通用输入组件工厂"""
        layout = QVBoxLayout()
        layout.setSpacing(6)

        label_h = QHBoxLayout()
        t_label = QLabel(title)
        t_label.setObjectName("InputTitle")
        h_label = QLabel(hint)
        h_label.setObjectName("InputHint")
        label_h.addWidget(t_label)
        label_h.addStretch()
        label_h.addWidget(h_label)

        edit_h = QHBoxLayout()
        edit_h.setSpacing(10)
        line_edit = DropLineEdit()
        line_edit.setPlaceholderText("选择路径或将文件夹/文件拖入此处...")
        btn = QPushButton("📂 浏览")
        btn.setFixedWidth(90)
        btn.clicked.connect(callback)

        edit_h.addWidget(line_edit)
        edit_h.addWidget(btn)

        layout.addLayout(label_h)
        layout.addLayout(edit_h)
        parent_layout.addLayout(layout)
        return line_edit

    def _apply_style(self):
        self.setStyleSheet("""
            QWidget { background-color: #F3F4F6; font-family: 'Segoe UI', 'Microsoft YaHei'; }
            QLabel { background: transparent; }
            #HeaderLabel { font-size: 24px; font-weight: 800; color: #111827; }
            #DescLabel { font-size: 13px; color: #6B7280; }

            #ConfigCard { background-color: white; border: 1px solid #E5E7EB; border-radius: 15px; }
            #InputTitle { font-size: 14px; font-weight: 700; color: #374151; }
            #InputHint { font-size: 12px; color: #9CA3AF; font-style: italic; }

            #SectionTitle { font-size: 13px; font-weight: 700; color: #4B5563; margin-top: 10px; }

            QLineEdit { border: 1px solid #D1D5DB; border-radius: 8px; padding: 10px; background: white; color: #111827; }
            QLineEdit:focus { border: 2px solid #6366F1; }

            QPushButton { background-color: white; border: 1px solid #D1D5DB; border-radius: 8px; padding: 8px; font-weight: bold; color: #374151; }
            QPushButton:hover { background-color: #F9FAFB; border-color: #9CA3AF; }

            #PrimaryBtn { background-color: #6366F1; color: white; border: none; font-size: 16px; border-radius: 12px; }
            #PrimaryBtn:hover { background-color: #4F46E5; }
            #PrimaryBtn:disabled { background-color: #C7D2FE; }

            #LogArea { 
                background-color: #1F2937; 
                color: #F9FAFB; 
                border-radius: 10px; 
                padding: 10px; 
                font-family: 'Consolas', 'Monaco', monospace; 
                font-size: 12px;
                line-height: 1.5;
            }
        """)

    # --- 槽函数实现 ---

    def select_daily_folder(self):
        p = QFileDialog.getExistingDirectory(self, "选择主表文件夹")
        if p: self.path_daily.setText(os.path.normpath(p))

    def select_baodan_folder(self):
        p = QFileDialog.getExistingDirectory(self, "选择副表文件夹")
        if p: self.path_baodan.setText(os.path.normpath(p))

    def select_output_file(self):
        default_path = os.path.join(os.path.expanduser('~'), 'Desktop', 'wide_report.csv')
        p, _ = QFileDialog.getSaveFileName(self, "保存计算结果", default_path, "CSV Files (*.csv)")
        if p: self.path_output.setText(os.path.normpath(p))

    def start_data_task(self):
        """启动逻辑保持不变，只需读取新的变量名"""
        p_daily = self.path_daily.text().strip()
        p_baodan = self.path_baodan.text().strip()
        p_out = self.path_output.text().strip()

        if not all([p_daily, p_baodan, p_out]):
            return QMessageBox.warning(self, "提示", "请完整填写所有路径。")

        self.start_btn.setEnabled(False)
        self.start_btn.setText("⚡ 正在进行核心计算...")
        self.log_output.clear()
        self.log_output.append("<span style='color: #10B981;'>[SYSTEM] 任务初始化成功，正在启动后台计算引擎...</span>")

        # 1. 实例化 Worker，传入所有参数
        worker = DataWorker(
            output_path=p_out,
            base_path_daily=p_daily,
            file_prefix_daily=self.DEFAULT_PREFIX_DAILY,
            base_path_baodan=p_baodan,
            file_prefix_baodan=self.DEFAULT_PREFIX_BAODAN
        )

        # 2. 连接信号
        worker.signals.finished.connect(self.task_finished)
        worker.signals.error.connect(self.task_error)
        worker.signals.result.connect(self.task_result)
        worker.signals.progress.connect(self.update_log)

        # 3. 将 Worker 提交给线程池
        self.threadpool.start(worker)

    def update_log(self, message: str):
        """更新日志（进度）信息"""
        self.log_output.append(message)
        self.log_output.verticalScrollBar().setValue(self.log_output.verticalScrollBar().maximum())

    def task_finished(self):
        """Worker 任务结束时调用 (无论成功或失败)"""
        self.update_log("--- 任务流程结束 ---")
        self.start_btn.setEnabled(True)
        self.start_btn.setText("🚀 启动数据处理与计算 (运行)")

    def task_error(self, message: str):
        """Worker 任务报告错误时调用"""
        self.log_output.append(f"<font color='#C0392B'>🔴 {message}</font>")  # 深红色错误

    def task_result(self, df: pd.DataFrame):
        """Worker 任务成功完成时调用"""
        self.log_output.append(
            f"<font color='#27AE60'>🎉 数据处理成功！最终 DataFrame 包含 {len(df)} 行数据，文件已保存。</font>")  # 翠绿色成功



class CrawlerPage(QWidget):
    # 页面名称，方便在主窗口中识别
    PAGE_NAME = "风神离职数据"
    DESC = "自动化获取骑手离职历史记录详情并导出"

    def __init__(self, parent=None):
        super().__init__(parent)

        self.worker: CrawlerWorker = None

        self._setup_ui()
        self.apply_styles()

    def apply_styles(self):
        """应用 Apple 风格的现代 UI 样式 (修复文字背景色问题)"""

        # Apple Design System Colors
        BG_COLOR = "#F5F5F7"  # 窗口大背景 (浅灰)
        CARD_COLOR = "#FFFFFF"  # 卡片背景 (纯白)
        TEXT_PRIMARY = "#1D1D1F"  # 主要文字
        TEXT_SECONDARY = "#86868B"  # 次要文字
        ACCENT_BLUE = "#0071E3"  # 苹果蓝
        ACCENT_RED = "#FF3B30"  # 苹果红
        INPUT_BG = "#F5F5F7"  # 输入框背景

        self.setStyleSheet(f"""
            /* 1. 全局基础样式 */
            QWidget {{
                background-color: {BG_COLOR}; 
                color: {TEXT_PRIMARY};
                font-family: "SF Pro Text", "Helvetica Neue", "Microsoft YaHei", sans-serif;
            }}

            /* 2. 【关键修复】强制所有标签背景透明 */
            QLabel {{
                background-color: transparent;
            }}

            /* 3. 核心卡片容器 (纯白背景) */
            #MainContainer {{
                background-color: {CARD_COLOR};
                border-radius: 20px;
                /* 阴影已通过 Python 代码添加 */
            }}

            /* 4. 标题样式 */
            .page_title {{
                color: {TEXT_PRIMARY};
                font-size: 22pt;
                font-weight: 600; 
            }}

            /* 5. 输入框上方的小标题 */
            .input_label {{
                font-size: 11pt;
                font-weight: 500;
                color: {TEXT_PRIMARY};
                margin-bottom: 4px; /* 让标签和输入框稍微近一点 */
            }}

            /* 6. 输入框 (Apple 风格：浅灰底，无边框) */
            QLineEdit {{
                border: none;
                border-radius: 10px;
                padding: 12px 16px;
                background-color: {INPUT_BG}; 
                color: {TEXT_PRIMARY};
                font-size: 11pt;
            }}
            QLineEdit:focus {{
                background-color: #E8E8ED; /* 聚焦时稍微变深 */
            }}

            /* 7. 进度条 */
            QProgressBar {{
                border: none;
                border-radius: 4px;
                background-color: #E5E5EA; 
                height: 6px;
                text-align: center;
            }}
            QProgressBar::chunk {{
                background-color: {ACCENT_BLUE}; 
                border-radius: 4px;
            }}

            /* 8. 状态文字 */
            #StatusLabel {{
                font-size: 10pt;
                color: {TEXT_SECONDARY};
            }}

            /* 9. 按钮基础 */
            QPushButton {{
                border-radius: 18px; 
                padding: 10px 24px;
                font-size: 11pt;
                font-weight: 600; /* 字体加粗一点更有质感 */
                border: none;
            }}

            /* 开始按钮 */
            #StartButton {{
                background-color: {ACCENT_BLUE};
                color: white;
            }}
            #StartButton:hover {{ background-color: #0077ED; }}
            #StartButton:pressed {{ background-color: #006ED6; }}

            /* 停止按钮 (浅红背景 + 深红文字) */
            #StopButton {{
                background-color: rgba(255, 59, 48, 0.1); 
                color: {ACCENT_RED};
            }}
            #StopButton:hover {{ background-color: rgba(255, 59, 48, 0.15); }}

            /* 打开文件夹按钮 (幽灵按钮风格) */
            #OpenButton {{
                background-color: #F2F2F7;
                color: {TEXT_PRIMARY};
            }}
            #OpenButton:hover {{ background-color: #E5E5EA; }}

            /* 禁用状态 */
            QPushButton:disabled {{
                background-color: #F5F5F7;
                color: #D1D1D6;
            }}
        """)

    def _setup_ui(self):
        # 外层布局用于居中
        outer_layout = QVBoxLayout(self)
        outer_layout.setContentsMargins(0, 0, 0, 0)

        # 弹簧将卡片顶到中间
        outer_layout.addStretch()

        # --- 核心卡片容器 ---
        self.container_widget = QFrame()
        self.container_widget.setObjectName("MainContainer")
        self.container_widget.setFixedWidth(680)  # 稍微加宽
        # 添加投影特效 (Shadow)
        shadow = QGraphicsDropShadowEffect(self)
        shadow.setBlurRadius(40)
        shadow.setXOffset(0)
        shadow.setYOffset(10)
        shadow.setColor(QColor(0, 0, 0, 20))  # 非常淡的黑色阴影
        self.container_widget.setGraphicsEffect(shadow)

        container_layout = QVBoxLayout(self.container_widget)
        container_layout.setContentsMargins(50, 50, 50, 50)  # 内部大留白
        container_layout.setSpacing(30)  # 元素间距拉大

        # 1. 标题 (居中)
        title_box = QHBoxLayout()
        title_icon = QLabel("🍃")  # 用 Emoji 或者图标
        title_icon.setStyleSheet("font-size: 28pt; background: transparent;")
        title = QLabel("离职数据导出")
        title.setObjectName("page_title")
        title_box.addStretch()
        title_box.addWidget(title_icon)
        title_box.addWidget(title)
        title_box.addStretch()
        container_layout.addLayout(title_box)

        container_layout.addSpacing(10)

        # 2. 文件名输入 (垂直排列更现代)
        input_layout = QVBoxLayout()
        input_layout.setSpacing(10)

        lbl_filename = QLabel("导出文件名")
        lbl_filename.setObjectName("input_label")

        self.entry_filename = QLineEdit()
        self.entry_filename.setPlaceholderText("输入文件名...")
        default_name = datetime.now().strftime("风神离职数据_%Y%m%d")
        self.entry_filename.setText(default_name)

        input_layout.addWidget(lbl_filename)
        input_layout.addWidget(self.entry_filename)
        container_layout.addLayout(input_layout)

        container_layout.addSpacing(10)

        # 3. 按钮区域 (水平排列，大按钮)
        btn_layout = QHBoxLayout()
        btn_layout.setSpacing(15)

        self.btn_start = QPushButton("开始导出")
        self.btn_start.setObjectName("StartButton")
        self.btn_start.setCursor(Qt.CursorShape.PointingHandCursor)
        self.btn_start.setFixedHeight(45)  # 加高按钮
        # 绑定槽函数
        self.btn_start.clicked.connect(self.start_crawler)

        self.btn_stop = QPushButton("停止")
        self.btn_stop.setObjectName("StopButton")
        self.btn_stop.setCursor(Qt.CursorShape.PointingHandCursor)
        self.btn_stop.setFixedHeight(45)
        # 绑定槽函数
        self.btn_stop.clicked.connect(self.stop_crawler)
        self.btn_stop.setEnabled(False)
        self.btn_stop.setFixedWidth(100)  # 停止按钮稍微小一点

        btn_layout.addWidget(self.btn_start)
        btn_layout.addWidget(self.btn_stop)
        container_layout.addLayout(btn_layout)

        # 4. 进度条与状态 (分离布局)
        status_layout = QVBoxLayout()
        status_layout.setSpacing(15)

        self.progress_bar = QProgressBar()
        self.progress_bar.setValue(0)
        self.progress_bar.setTextVisible(False)  # 隐藏进度文字，更极简

        self.lbl_status = QLabel("准备就绪")
        self.lbl_status.setObjectName("StatusLabel")
        self.lbl_status.setAlignment(Qt.AlignmentFlag.AlignCenter)

        status_layout.addWidget(self.lbl_status)  # 文字在上方
        status_layout.addWidget(self.progress_bar)  # 细条在下方

        container_layout.addLayout(status_layout)

        # 5. 打开文件按钮 (作为底部链接式按钮)
        link_layout = QHBoxLayout()
        self.btn_open = QPushButton("📂 打开导出文件夹")
        self.btn_open.setObjectName("OpenButton")
        self.btn_open.setCursor(Qt.CursorShape.PointingHandCursor)
        self.btn_open.setFlat(True)  # 扁平化
        # 绑定槽函数
        self.btn_open.clicked.connect(self.open_output_directory)
        self.btn_open.setEnabled(False)
        link_layout.addStretch()
        link_layout.addWidget(self.btn_open)
        link_layout.addStretch()
        container_layout.addLayout(link_layout)

        # 将卡片添加到外层
        outer_layout.addWidget(self.container_widget, alignment=Qt.AlignmentFlag.AlignCenter)
        outer_layout.addStretch()



    # --- 槽函数 (Slots) ---

    @Slot()
    def start_crawler(self):
        """处理开始爬取按钮点击事件"""
        # 检查文件名输入
        filename = self.entry_filename.text().strip()
        if not filename:
            QMessageBox.warning(self, "输入错误", "请输入有效的导出文件名。")
            return

        # 构造完整的输出路径
        # 统一将文件导出到程序运行目录下的 'output' 文件夹
        output_dir = os.path.join(os.getcwd(), 'output')
        os.makedirs(output_dir, exist_ok=True)
        full_path = os.path.join(output_dir, f"{filename}.csv")

        # 状态更新
        self.lbl_status.setText("正在初始化爬虫...")
        self.progress_bar.setValue(0)
        self.btn_start.setEnabled(False)
        self.btn_stop.setEnabled(True)
        self.btn_open.setEnabled(False)

        # 实例化并启动工作线程
        self.worker = CrawlerWorker(output_filename=full_path)

        # 连接 Worker 的信号到当前页面的槽
        self.worker.signals.progress_signal.connect(self.update_progress)
        self.worker.signals.error_signal.connect(self.handle_error)
        self.worker.signals.success_signal.connect(self.handle_success)

        self.worker.start()
        self.lbl_status.setText("任务已启动，正在获取数据...")

    @Slot()
    def stop_crawler(self):
        """安全停止爬虫线程"""
        if self.worker and self.worker.is_alive():
            # 假设 CrawlerWorker 实现了 stop 方法
            if hasattr(self.worker, 'stop'):
                self.worker.stop()
                self.lbl_status.setText("🛑 正在尝试停止任务，请稍候...")
            else:
                # 如果 Worker 没有 stop 方法，我们只能等待它自然结束
                self.lbl_status.setText("🛑 任务将等待当前请求完成后自然停止...")

    @Slot(int, int)
    def update_progress(self, current_page: int, total_pages: int):
        """更新进度条和状态文本"""
        if total_pages == 0:
            percentage = 0
        else:
            percentage = int((current_page / total_pages) * 100)

        self.progress_bar.setValue(percentage)
        self.lbl_status.setText(f"爬取中... 进度: 第 {current_page}/{total_pages} 页")

    @Slot(str)
    def handle_error(self, message: str):
        """处理爬虫线程返回的错误"""
        self._reset_ui(False)
        self.lbl_status.setStyleSheet("color: #E74C3C; font-weight: bold;")
        self.lbl_status.setText(f"❌ 任务失败：{message}")
        QMessageBox.critical(self, "爬虫任务错误", message)

    @Slot(str, int)
    def handle_success(self, file_path: str, count: int):
        """处理爬虫线程返回的成功结果"""
        self._reset_ui(True)

        if count > 0:
            self.lbl_status.setStyleSheet("color: #2ECC71; font-weight: bold;")
            self.lbl_status.setText(f"✅ 任务完成！成功导出 {count} 条记录到：{os.path.basename(file_path)}")
            self.btn_open.setEnabled(True)
            self.last_output_path = file_path
            QMessageBox.information(self, "任务完成", f"数据已成功导出！共 {count} 条记录。")
        else:
            self.lbl_status.setStyleSheet("color: #F39C12; font-weight: bold;")
            self.lbl_status.setText("⚠️ 任务完成，但未找到匹配的数据记录。")

        self.progress_bar.setValue(100)

    @Slot()
    def open_output_directory(self):
        """打开最后一次导出文件所在的目录"""
        if hasattr(self, 'last_output_path') and os.path.exists(self.last_output_path):
            directory = os.path.dirname(self.last_output_path)

            # 使用 OS 命令打开目录
            if os.name == 'nt':  # Windows
                os.startfile(directory)
            elif os.uname().sysname == 'Darwin':  # macOS
                os.system(f'open "{directory}"')
            else:  # Linux
                os.system(f'xdg-open "{directory}"')
        else:
            QMessageBox.warning(self, "文件未找到", "没有找到最近导出的文件路径。")

    def _reset_ui(self, success: bool):
        """重置 UI 状态"""
        self.btn_start.setEnabled(True)
        self.btn_stop.setEnabled(False)
        self.lbl_status.setStyleSheet("color: #7f8c8d;")  # 恢复默认样式
        # 如果是成功，文件打开按钮在 handle_success 中启用
        if not success:
            self.btn_open.setEnabled(False)


# 统一的信号托管类 (SignalHost) 必须在 worker 之前定义
class SignalHost(QObject):
    success_signal = Signal(str, int)  # (文件路径, 记录总数)
    error_signal = Signal(str)         # (错误消息)
    progress_signal = Signal(int, int) # (当前页/当前步骤, 总页数/总步骤)


# 尝试导入真实的 Worker 类
try:
    # 假设 CrawlerWorker 位于 CrawlerWorker.py 文件中
    from CrawlerWorker import CrawlerWorker
    # 假设 Worker 位于 worker.py 文件中
    from worker import Worker

    print("Successful import of all actual Worker classes.")

except ImportError as e:
    # 如果任何一个导入失败，则进入此块，并模拟所有缺失的 Worker
    print(f"Warning: Failed to import actual Worker classes ({e}). Using simulated workers for testing.")


    # 1. 模拟 Worker (通用 Worker)
    class Worker(QObject):  # ✅ 继承 QObject
        """模拟通用 Worker 的接口"""
        finished = Signal(str)
        error = Signal(str)

        def __init__(self, f1, f2, output_dir):
            super().__init__()
            pass

        def start(self):
            print("Simulated Worker (Generic) started.")
            # 模拟成功返回
            # self.finished.emit("C:/output/result.csv")
            pass

        def stop(self):
            pass


    # 2. 模拟 CrawlerWorker (爬虫 Worker)
    class CrawlerWorker(QObject):  # ✅ 继承 QObject
        """模拟 CrawlerWorker 的接口，用于调试和测试"""
        signals: SignalHost = None

        def __init__(self, output_filename: str):
            super().__init__()
            self.signals = SignalHost()  # 必须实例化 SignalHost
            self.output_filename = output_filename

        def start(self):
            print("Simulated CrawlerWorker started.")
            # 模拟成功信号，以测试 CrawlerPage 的 UI 响应
            self.signals.success_signal.emit(os.path.abspath(self.output_filename), 100)

        def stop(self):
            print("Simulated CrawlerWorker stop requested.")
            # 可以在这里模拟发送取消错误
            # self.signals.error_signal.emit("任务被模拟取消。")
            pass

# --- 辅助类：实现页面切换时的淡入淡出效果 ---
class FadeStackedWidget(QStackedWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.current_index = -1

    def setCurrentIndex(self, index):
        if self.current_index == index:
            return

        new_widget = self.widget(index)

        self.opacity_effect = QGraphicsOpacityEffect(new_widget)
        new_widget.setGraphicsEffect(self.opacity_effect)

        self.anim = QPropertyAnimation(self.opacity_effect, b"opacity")
        self.anim.setDuration(300)
        self.anim.setStartValue(0.0)
        self.anim.setEndValue(1.0)
        self.anim.setEasingCurve(QEasingCurve.Type.InOutQuad)
        self.anim.finished.connect(lambda: self._animation_finished(new_widget))

        super().setCurrentIndex(index)
        new_widget.show()
        self.anim.start()
        self.current_index = index

    def _animation_finished(self, widget):
        if widget.graphicsEffect():
            widget.setGraphicsEffect(None)


# --- 1. 业务功能页面：CSV合并页面 (MergePage) ---

class MergePage(QWidget):
    PAGE_NAME = "B端数据处理"
    DESC = "智能合并商户明细与商智核本地CSV报表"
    pass_index_to_main = Signal(int)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.desktop_dir = os.path.join(os.path.expanduser("~"), "Desktop")
        self.worker = None
        self._setup_ui()
        self.apply_styles()

    def apply_styles(self):
        """应用现代化样式，移除输入框阴影，只保留焦点边框高亮"""
        PRIMARY_COLOR = "#007AFF"
        SUCCESS_COLOR = "#34C759"
        BACKGROUND_COLOR = "#F0F2F5"

        self.setStyleSheet(f"""
            QWidget {{ background-color: {BACKGROUND_COLOR}; }}

            QLabel {{ color: #2C3E50; font-family: "Microsoft YaHei"; }}

            /* 标题 */
            #TitleLabel {{ color: {PRIMARY_COLOR}; margin-bottom: 10px; }}

            /* 输入框 (移除阴影，只保留焦点边框高亮) */
            QLineEdit {{
                border: 1px solid #D1D5DA; /* 默认边框 */
                border-radius: 6px;
                padding: 8px 10px;
                background-color: white;
                font-size: 10pt;
                /* 移除了 box-shadow */
                transition: border-color 0.2s; /* 仅保留边框颜色过渡 */
            }}
            QLineEdit:focus {{ /* 焦点效果 */
                border: 1px solid {PRIMARY_COLOR}; /* 高亮边框颜色 */
                box-shadow: 0 0 5px rgba(0, 122, 255, 0.3); /* 柔和的焦点光晕 */
            }}

            /* 浏览按钮 */
            .BrowseButton {{
                background-color: #3498db;
                color: white;
                border-radius: 6px;
                font-size: 10pt;
                padding: 0 10px;
            }}
            .BrowseButton:hover {{
                background-color: #2980b9;
            }}

            /* 运行按钮 */
            #RunButton {{
                background-color: {SUCCESS_COLOR};
                color: white;
                border-radius: 10px;
                padding: 12px;
                font-weight: bold;
                box-shadow: 0 4px 12px rgba(52, 199, 89, 0.3);
                transition: background-color 0.2s, box-shadow 0.2s;
            }}
            #RunButton:hover {{
                background-color: #2CAE4E;
                box-shadow: 0 6px 15px rgba(52, 199, 89, 0.4);
            }}
            #RunButton:disabled {{
                background-color: #C8C8C8;
                box-shadow: none;
            }}
        """)

    def _setup_ui(self):
        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(40, 30, 40, 30)
        main_layout.setSpacing(20)

        # 标题
        title_label = QLabel("订单数据合并工具")
        title_label.setObjectName("TitleLabel")
        title_font = QFont("Microsoft YaHei", 24, QFont.Bold)
        title_label.setFont(title_font)
        title_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        main_layout.addWidget(title_label)

        # 描述
        desc_label = QLabel("请依次选择两个 CSV 文件和结果输出目录，点击开始合并。")
        desc_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        desc_label.setStyleSheet("color: #7f8c8d; font-size: 10pt; margin-bottom: 15px;")
        main_layout.addWidget(desc_label)

        # 文件选择控件
        self.entry_f1 = self.create_file_selector(
            "步骤 1: 选择【有效商户明细】文件 (.csv)", "选择有效商户明细文件", self.select_file, "*.csv"
        )
        self.entry_f2 = self.create_file_selector(
            "步骤 2: 选择【商智核/超抢手】文件 (.csv)", "选择商智核/超抢手文件", self.select_file, "*.csv"
        )
        self.frame_output, self.entry_output = self.create_file_selector(
            "步骤 3: 选择输出结果保存路径 (文件夹)", "选择输出文件夹", self.select_directory, is_directory=True
        )
        self.entry_output.setText(self.desktop_dir)

        main_layout.addWidget(self.create_separator())

        # 运行按钮
        self.btn_run = QPushButton("🚀 开始合并并导出")
        self.btn_run.setObjectName("RunButton")
        self.btn_run.setFont(QFont("Microsoft YaHei", 15, QFont.Bold))
        self.btn_run.setFixedHeight(60)
        self.btn_run.clicked.connect(self.start_processing)
        main_layout.addWidget(self.btn_run)

        # 状态标签
        self.lbl_status = QLabel("准备就绪")
        self.lbl_status.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.lbl_status.setFont(QFont("Microsoft YaHei", 10))
        self.lbl_status.setStyleSheet("color: #95a5a6; padding-top: 5px;")
        main_layout.addWidget(self.lbl_status)

        main_layout.addStretch()

    def create_separator(self):
        line = QLabel()
        line.setFixedHeight(1)
        line.setStyleSheet("background-color: #D1D5DA; margin-top: 5px; margin-bottom: 5px;")
        return line

    def create_file_selector(self, label_text, dialog_title, command_func, filetypes=None, is_directory=False):
        vbox = QVBoxLayout()
        vbox.setSpacing(5)

        label = QLabel(label_text)
        label.setStyleSheet("color: #34495e; font-weight: 500; font-size: 11pt;")
        vbox.addWidget(label)

        hbox = QHBoxLayout()
        hbox.setContentsMargins(0, 0, 0, 0)
        hbox.setSpacing(10)

        entry = QLineEdit()
        entry.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)
        entry.setReadOnly(True)
        entry.setFixedHeight(36)

        button_text = "选择目录" if is_directory else "浏览文件"
        btn = QPushButton(button_text)
        btn.setObjectName("BrowseButton")
        btn.setFixedWidth(100)
        btn.setFixedHeight(36)
        btn.clicked.connect(lambda: command_func(entry, dialog_title, filetypes))

        hbox.addWidget(entry)
        hbox.addWidget(btn)

        vbox.addLayout(hbox)
        self.layout().addLayout(vbox)

        if is_directory:
            return vbox, entry
        else:
            return entry

    def select_file(self, entry_widget, title, filetypes):
        file_filter = f"CSV 文件 ({filetypes});;所有文件 (*.*)"
        filename, _ = QFileDialog.getOpenFileName(self, title, self.desktop_dir, file_filter)
        if filename:
            entry_widget.setText(filename)

    def select_directory(self, entry_widget, title, *args):
        initial_dir = entry_widget.text() or self.desktop_dir
        directory = QFileDialog.getExistingDirectory(self, title, initial_dir, QFileDialog.Option.ShowDirsOnly)
        if directory:
            entry_widget.setText(directory)

    def start_processing(self):
        f1 = self.entry_f1.text()
        f2 = self.entry_f2.text()
        output_dir = self.entry_output.text()

        if not all([f1, f2, output_dir]) or not os.path.exists(f1) or not os.path.exists(f2) or not os.path.isdir(
                output_dir):
            QMessageBox.critical(self, "错误", "请检查所有文件和路径是否已正确选择或存在。")
            return

        self.btn_run.setEnabled(False)
        self.btn_run.setText("⏳ 正在处理中...")
        self.lbl_status.setText("正在读取和计算数据，请稍候...")
        self.lbl_status.setStyleSheet("color: #f39c12;")

        try:
            self.worker = Worker(f1, f2, output_dir)
            self.worker.finished.connect(self.on_success)
            self.worker.error.connect(self.on_error)
            self.worker.start()
        except NameError:
            QMessageBox.critical(self, "错误", "缺少 worker.py 文件，无法启动处理线程。")
            self.btn_run.setEnabled(True)
            self.btn_run.setText("🚀 开始合并并导出")

    def on_success(self, path):
        self.btn_run.setEnabled(True)
        self.btn_run.setText("🚀 开始合并并导出")
        self.lbl_status.setText("✅ 处理完成！")
        self.lbl_status.setStyleSheet("color: #2ecc71; font-weight: bold;")
        QMessageBox.information(self, "成功", f"文件已生成！\n\n保存在：\n{path}")

    def on_error(self, error_msg):
        self.btn_run.setEnabled(True)
        self.btn_run.setText("🚀 开始合并并导出")
        self.lbl_status.setText("❌ 处理出错")
        self.lbl_status.setStyleSheet("color: #e74c3c; font-weight: bold;")
        QMessageBox.critical(self, "处理失败", error_msg)


class ElideLabel(QLabel):
    """支持单行省略的标签，彻底解决背景阴影块问题"""

    def __init__(self, text="", parent=None):
        super().__init__(text, parent)
        self._full_text = text
        self.setStyleSheet("background: transparent; border: none;")

    def set_text_raw(self, text):
        self._full_text = text
        self.update_elide()

    def update_elide(self):
        # 核心：根据当前标签宽度计算省略文本
        fm = QFontMetrics(self.font())
        elided = fm.elidedText(self._full_text, Qt.ElideRight, self.width())
        super().setText(elided)

    def resizeEvent(self, event):
        self.update_elide()
        super().resizeEvent(event)


class CompactCard(QFrame):
    clicked = Signal()

    def __init__(self, title, desc, icon_name, accent_color, parent=None):
        super().__init__(parent)
        self.setObjectName("CompactCard")
        self.accent_color = accent_color
        self.setFixedSize(280, 100)
        self.setCursor(Qt.CursorShape.PointingHandCursor)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(20, 15, 20, 15)
        layout.setSpacing(8)

        # 标题行
        title_layout = QHBoxLayout()
        title_layout.setSpacing(10)
        self.icon_label = QLabel()
        self.icon_label.setPixmap(qta.icon(icon_name, color=accent_color).pixmap(20, 20))
        self.icon_label.setStyleSheet("background: transparent;")

        self.title_label = QLabel(title)
        self.title_label.setStyleSheet("font-size: 11pt; font-weight: 600; color: #1D1D1F; background: transparent;")

        title_layout.addWidget(self.icon_label)
        title_layout.addWidget(self.title_label)
        title_layout.addStretch()

        # 描述行
        self.desc_label = ElideLabel(desc)
        self.desc_label.setStyleSheet("font-size: 9pt; color: #86868B; background: transparent;")

        layout.addLayout(title_layout)
        layout.addWidget(self.desc_label)
        layout.addStretch()
        self._set_style(False)

    def _set_style(self, hovered: bool):
        border_color = self.accent_color if hovered else "#E5E5E7"
        bg_color = "#FBFBFD" if hovered else "#FFFFFF"
        self.setStyleSheet(
            f"#CompactCard {{ background-color: {bg_color}; border: 1px solid {border_color}; border-radius: 10px; }}")

    def enterEvent(self, event): self._set_style(True)

    def leaveEvent(self, event): self._set_style(False)

    def mousePressEvent(self, event):
        if event.button() == Qt.MouseButton.LeftButton: self.clicked.emit()


class HomePage(QWidget):
    PAGE_NAME = "首页"
    navigate_to_page = Signal(str)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.main_window = parent  # 必须在 MainWindow 实例化时传入 self
        self._setup_ui()

    def _setup_ui(self):
        self.setStyleSheet("background-color: #F5F5F7;")
        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(50, 40, 50, 40)
        main_layout.setSpacing(30)

        header = QVBoxLayout()
        title = QLabel("工作台")
        title.setStyleSheet("font-size: 24pt; font-weight: 700; color: #1D1D1F; background: transparent;")
        subtitle = QLabel("选择下方工具卡片快速启动任务")
        subtitle.setStyleSheet("font-size: 10pt; color: #86868B; background: transparent;")
        header.addWidget(title)
        header.addWidget(subtitle)
        main_layout.addLayout(header)

        grid_container = QWidget()
        grid_container.setStyleSheet("background: transparent;")
        self.grid_layout = QGridLayout(grid_container)
        self.grid_layout.setSpacing(20)
        self.grid_layout.setAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignTop)

        # 自动生成逻辑
        if self.main_window and hasattr(self.main_window, 'page_config'):
            card_index = 0
            colors = ["#007AFF", "#34C759", "#5856D6", "#FF9500", "#FF2D55", "#AF52DE"]

            for conf in self.main_window.page_config:
                name = conf["name"]
                if name in ["首页", "设置"]: continue

                # 【修复 1】安全读取 DESC
                desc = getattr(conf["class"], "DESC", f"执行{name}相关自动化操作")

                card = CompactCard(name, desc, conf["icon"], colors[card_index % len(colors)])

                # 【修复 2关键】使用默认参数绑定当前变量，解决跳转失效问题
                card.clicked.connect(lambda target=name: self.navigate_to_page.emit(target))

                self.grid_layout.addWidget(card, card_index // 3, card_index % 3)
                card_index += 1

        main_layout.addWidget(grid_container)
        main_layout.addStretch()

# --- 5. 主窗口：使用侧边栏布局 (MainWindow) ---

class MainWindow(QMainWindow):
    # 【核心 1】定义信号，必须定义在类属性中（__init__ 之外）
    sig_status_update = Signal(str, str)

    def __init__(self):
        super().__init__()
        self.setWindowTitle("Intelligent Middle Platform v2.0")
        self.resize(1200, 780)  # 稍微收窄宽度，让侧边栏和两列卡片布局更紧凑
        self.setMinimumSize(950, 650)

        self.buttons = []
        self.page_names_to_index = {}

        # 页面配置
        self.page_config = [
            {"name": "首页", "icon": "fa5s.home", "class": HomePage},
            {"name": "风神离职数据", "icon": "fa5s.user-minus", "class": CrawlerPage},
            {"name": "批量数据导出", "icon": "fa5s.cloud-download-alt", "class": BatchExportPage},
            {"name": "B端数据处理", "icon": "fa5s.project-diagram", "class": MergePage},
            {"name": "宽表导出", "icon": "fa5s.th-list", "class": WideTablePage},
            {"name": "CSV极速导出", "icon": "fa5s.file-excel", "class": XlsxToCsvPage},
            {"name": "设置", "icon": "fa5s.cog", "class": SettingsPage},
        ]

        self._setup_ui()
        self._setup_tray()
        self._init_status_bar()
        self.apply_external_style()

        # 【核心 2】连接信号到 UI 更新函数
        self.sig_status_update.connect(self.update_cookie_status)

        # 启动后延迟检测
        QTimer.singleShot(1000, self.check_cookie_realtime)

    def apply_external_style(self):
        style_path = os.path.join(os.path.dirname(__file__), "style.qss")
        if os.path.exists(style_path):
            with open(style_path, "r", encoding="utf-8") as f:
                self.setStyleSheet(f.read())

    def _setup_ui(self):
        central_widget = QWidget()
        self.main_layout = QHBoxLayout(central_widget)
        self.main_layout.setContentsMargins(0, 0, 0, 0)
        self.main_layout.setSpacing(0)

        # 侧边栏
        self.sidebar = QWidget()
        self.sidebar.setObjectName("SideBar")
        self.sidebar.setFixedWidth(200)
        self.nav_layout = QVBoxLayout(self.sidebar)
        self.nav_layout.setContentsMargins(0, 15, 0, 15)
        self.nav_layout.setSpacing(2)

        # 内容区
        self.content_outer_wrapper = QVBoxLayout()
        self.content_outer_wrapper.setContentsMargins(15, 15, 15, 15)
        self.content_container = QWidget()
        self.content_container.setObjectName("ContentCanvas")
        self.container_inner_layout = QVBoxLayout(self.content_container)
        self.container_inner_layout.setContentsMargins(0, 0, 0, 0)

        shadow = QGraphicsDropShadowEffect(self)
        shadow.setBlurRadius(20)
        shadow.setColor(QColor(0, 0, 0, 35))
        shadow.setOffset(0, 4)
        self.content_container.setGraphicsEffect(shadow)

        self.stacked_widget = QStackedWidget()
        self.container_inner_layout.addWidget(self.stacked_widget)
        self.content_outer_wrapper.addWidget(self.content_container)

        for i, conf in enumerate(self.page_config):
            # --- 【核心修改 1】实例化时传入 self ---
            # 这样 HomePage 内部可以通过 self.main_window 访问到 page_config
            page_inst = conf["class"](self)

            # --- 【核心修改 2】连接首页的跳转信号 ---
            # 检查页面实例是否有 navigate_to_page 信号 (通常只有 HomePage 有)
            if hasattr(page_inst, 'navigate_to_page'):
                page_inst.navigate_to_page.connect(self.navigate_to_page_by_name)

            index = self.stacked_widget.addWidget(page_inst)
            self.page_names_to_index[conf["name"]] = index

            # --- 侧边栏导航按钮保持原样 ---
            btn = QPushButton(f"  {conf['name']}")
            btn.setIcon(qta.icon(conf["icon"], color='#BDC3C7', color_active='#007AFF'))
            btn.setIconSize(QSize(18, 18))
            btn.setProperty("class", "NavButton")
            # 闭包安全写法推荐：idx=index
            btn.clicked.connect(lambda checked, idx=index: self.switch_page(idx))

            self.nav_layout.addWidget(btn)
            self.buttons.append(btn)

        self.nav_layout.addStretch()
        self.main_layout.addWidget(self.sidebar)
        self.main_layout.addLayout(self.content_outer_wrapper)
        self.setCentralWidget(central_widget)
        if self.buttons: self.switch_page(0)

    def _init_status_bar(self):
        """重新定义更现代的状态栏结构"""
        self.status_bar = QStatusBar()
        self.status_bar.setObjectName("ModernStatusBar")
        self.setStatusBar(self.status_bar)

        # 移除状态栏自带的分割线
        self.status_bar.setStyleSheet("QStatusBar::item { border: none; }")

        # 1. 状态标签容器 (药丸)
        self.badge_widget = QWidget()
        self.badge_widget.setObjectName("BadgeContainer")
        self.badge_layout = QHBoxLayout(self.badge_widget)
        self.badge_layout.setContentsMargins(12, 3, 12, 3)  # 减小垂直外边距
        self.badge_layout.setSpacing(6)

        self.status_icon = QLabel()
        self.status_text = QLabel("正在同步")
        self.status_text.setStyleSheet("font-family: 'Segoe UI', 'PingFang SC'; font-weight: 500; font-size: 12px;")

        self.badge_layout.addWidget(self.status_icon)
        self.badge_layout.addWidget(self.status_text)

        # 2. 辅助信息容器
        self.info_label = QLabel("Eleme Automator v2.0")
        self.info_label.setStyleSheet("color: #95A5A6; font-size: 11px; margin-right: 15px;")

        # 组装到状态栏
        self.status_bar.addWidget(self.badge_widget)
        self.status_bar.addPermanentWidget(self.info_label)

    @Slot(str, str)
    def update_cookie_status(self, status: str, text: str):
        """现代化的状态切换逻辑：更高级的配色方案"""
        # 调色盘：使用了更高级的莫兰迪色系/淡色系
        configs = {
            "valid": {
                "color": "#10B981", "bg": "rgba(16, 185, 129, 0.12)",
                "border": "rgba(16, 185, 129, 0.3)", "icon": "fa5s.check-circle"
            },
            "invalid": {
                "color": "#EF4444", "bg": "rgba(239, 68, 68, 0.12)",
                "border": "rgba(239, 68, 68, 0.3)", "icon": "fa5s.times-circle"
            },
            "loading": {
                "color": "#3B82F6", "bg": "rgba(59, 130, 246, 0.12)",
                "border": "rgba(59, 130, 246, 0.3)", "icon": "fa5s.sync-alt"
            },
            "error": {
                "color": "#F59E0B", "bg": "rgba(245, 158, 11, 0.12)",
                "border": "rgba(245, 158, 11, 0.3)", "icon": "fa5s.exclamation-triangle"
            }
        }
        cfg = configs.get(status, configs["error"])

        # 更新图标（带抗锯齿感）
        self.status_icon.setPixmap(qta.icon(cfg["icon"], color=cfg["color"]).pixmap(14, 14))
        self.status_text.setText(text)
        self.status_text.setStyleSheet(f"color: {cfg['color']}; background: transparent;")

        # 更新容器外观：使用更现代的边框和背景混合
        self.badge_widget.setStyleSheet(f"""
            QWidget#BadgeContainer {{
                background-color: {cfg['bg']};
                border: 1px solid {cfg['border']};
                border-radius: 14px;
                margin: 4px 0px;
            }}
        """)

        # 强制重绘
        self.badge_widget.style().unpolish(self.badge_widget)
        self.badge_widget.style().polish(self.badge_widget)

    def check_cookie_realtime(self):
        """骑手接口检测逻辑"""
        # 发射加载中信号
        self.sig_status_update.emit("loading", "正在检测")

        def run_check():
            try:
                # 假设 SettingsPage 有 get_all_cookies 静态方法
                cookies = SettingsPage.get_all_cookies()
                url = "https://httpizza.ele.me/lpd.meepo.mgmt/knight/queryKnightDimissionRecords"
                params = {'pageIndex': 1, 'pageSize': 20}

                res = requests.get(url, params=params, cookies=cookies, timeout=5)

                if res.status_code == 200:
                    data = res.json()
                    if str(data.get('code')) == '200':
                        # 【核心 4】子线程中仅通过信号通知 UI
                        self.sig_status_update.emit("valid", "已连接")
                    else:
                        self.sig_status_update.emit("invalid", "令牌失效")
                else:
                    self.sig_status_update.emit("invalid", f"错误 {res.status_code}")
            except Exception as e:
                print(f"Check Error Details: {e}")
                self.sig_status_update.emit("error", "网络超时")

        # 使用守护线程避免关闭窗口时卡死
        threading.Thread(target=run_check, daemon=True).start()

    def _setup_tray(self):
        self.tray_icon = QSystemTrayIcon(self)
        self.tray_icon.setIcon(qta.icon('fa5s.toolbox', color='#2C3E50'))
        menu = QMenu()
        menu.addAction("还原窗口", self.showNormal)
        menu.addAction("彻底退出", QApplication.instance().quit)
        self.tray_icon.setContextMenu(menu)
        self.tray_icon.show()

    def changeEvent(self, event):
        if event.type() == QEvent.WindowStateChange and self.windowState() & Qt.WindowMinimized:
            self.hide()
            event.accept()
            return
        super().changeEvent(event)

    @Slot(int)
    def switch_page(self, index):
        self.stacked_widget.setCurrentIndex(index)
        for i, btn in enumerate(self.buttons):
            btn.setProperty("active", i == index)
            btn.style().polish(btn)

    @Slot(str)
    def navigate_to_page_by_name(self, name):
        if name in self.page_names_to_index:
            self.switch_page(self.page_names_to_index[name])


# --- 应用程序启动 ---
if __name__ == "__main__":
    # 确保 QApplication 组织名和应用名设置在 QSettings 之前
    QApplication.setOrganizationName("YourCompanyName")
    QApplication.setApplicationName("AutomationToolbox")

    # 1. 创建 QApplication 实例 (只创建一次)
    app = QApplication(sys.argv)

    # 2. 设置全局字体 (配置应用)
    # 建议使用更现代的字体或与您的 Apple 风格匹配的字体栈
    app.setFont(QFont("Microsoft YaHei", 10))

    # 3. 实例化并显示主窗口
    window = MainWindow()
    window.show()

    # 4. 运行应用主循环
    sys.exit(app.exec())