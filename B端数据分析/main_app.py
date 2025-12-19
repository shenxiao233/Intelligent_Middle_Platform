import sys
import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional
from PySide6.QtWidgets import (
    QApplication, QMainWindow,QFrame,QGraphicsDropShadowEffect, QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QLineEdit, QGridLayout,QPushButton, QMessageBox, QFileDialog, QSizePolicy,
    QStackedWidget, QGraphicsOpacityEffect,QSpacerItem,QDateEdit,
    QProgressBar,QGroupBox
)
from PySide6.QtGui import QFont, QColor, QIcon
from PySide6.QtCore import Qt, Signal, QPropertyAnimation, QEasingCurve, QSize,QDate,QThread
from PySide6.QtCore import QSettings, QObject
from PySide6.QtWidgets import QTextEdit # 新增导入
import json # 新增导入
from PySide6.QtWidgets import QScrollArea # 确保在您的导入列表中
from PySide6.QtCore import Qt, Signal, QObject, Slot,QThreadPool
from PySide6.QtGui import QFont, QColor
import os
from data_worker import DataWorker
from xlsx_to_csv_page import XlsxToCsvPage

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



# --- 预设默认 Team IDs ---
DEFAULT_TEAM_IDS: List[int] = [17440957, 17440962, 17440963, 17440964, 17440965, 17440967, 917535482]
DEFAULT_TEAM_IDS_STR = ", ".join(map(str, DEFAULT_TEAM_IDS))

# --- 导入真实 Worker ---
# 假设您已经把真实的 Worker 代码放在 batch_exporter_worker.py 文件中
try:
    from batch_exporter_worker import BatchExporterWorker

    print("✅ 成功导入真实 BatchExporterWorker")
except ImportError:
    print("❌ 无法导入真实 BatchExporterWorker，使用占位符")


    # 占位符 Worker 定义 (您原来的模拟代码)
    class BatchExporterWorker(QObject):
        finished_single = Signal(str, str, str)
        finished_batch = Signal(str, str)
        progress_update = Signal(int, str)
        error_occurred = Signal(str)  # ✅ 修正为 (str)

        def __init__(self, output_dir, parent=None):
            super().__init__(parent)
            self.output_dir = output_dir
            self.is_running = True

        def set_export_parameters(self, team_ids, date_params, task_keys):
            pass

        @Slot(str)
        def run_single(self, task_key: str):
            QThread.msleep(1500)
            self.finished_single.emit(task_key, "成功", f"模拟文件_{task_key}.csv")

        @Slot()
        def run_batch(self):
            for i in range(4):
                if not self.is_running:
                    self.finished_batch.emit("取消", "")
                    return
                self.progress_update.emit(i + 1, f"模拟任务 {i + 1}")
                QThread.msleep(1000)
            self.finished_batch.emit("成功", self.output_dir)

        def stop(self):
            self.is_running = False


# --- 辅助函数 ---

def get_ui_date_string(date_edit: QDateEdit) -> str:
    """从 QDateEdit 控件获取 YYYY-MM-DD 格式的字符串"""
    return date_edit.date().toString("yyyy-MM-dd")

class CustomDateEdit(QDateEdit):
    """
    自定义 QDateEdit，完全禁用鼠标滚轮对日期的修改。
    """
    def wheelEvent(self, event):
        # 忽略所有滚轮事件，阻止值变化
        event.ignore()
        # 如果您还想阻止事件传播到父级，确保返回
        return


# --- 任务卡片类 (包含独立按钮和状态) ---

class TaskInputCard(QFrame):
    """
    用于单个导出任务的卡片式日期输入组件，
    包含独立的"导出"按钮和状态标签。
    """

    def __init__(self, title: str, task_key: str, start_date_default: QDate, end_date_default: QDate, parent=None):
        super().__init__(parent)
        self.task_key = task_key
        self.setObjectName("TaskCard")
        self.setFrameShape(QFrame.Shape.StyledPanel)
        self.setFrameShadow(QFrame.Shadow.Raised)
        self.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)

        layout = QVBoxLayout(self)
        layout.setSpacing(10)
        layout.setContentsMargins(15, 15, 15, 15)

        # 1. 标题
        lbl_title = QLabel(title)
        lbl_title.setStyleSheet("font-size: 13pt; font-weight: 600; color: #1D1D1F; background: transparent;")
        layout.addWidget(lbl_title)

        # 2. 日期选择区域
        date_grid = QGridLayout()
        date_grid.setSpacing(10)

        # 开始日期
        lbl_start = QLabel("开始日期")
        lbl_start.setObjectName("input_label")
        # --- 重点修改：使用 CustomDateEdit 替换 QDateEdit ---
        self.date_start = CustomDateEdit(start_date_default)
        self.date_start.setCalendarPopup(True)
        self.date_start.setDisplayFormat("yyyy-MM-dd")
        self.date_start.setObjectName("DateEdit")

        date_grid.addWidget(lbl_start, 0, 0)
        date_grid.addWidget(self.date_start, 1, 0)

        # 结束日期
        lbl_end = QLabel("结束日期")
        lbl_end.setObjectName("input_label")
        # --- 重点修改：使用 CustomDateEdit 替换 QDateEdit ---
        self.date_end = CustomDateEdit(end_date_default)
        self.date_end.setCalendarPopup(True)
        self.date_end.setDisplayFormat("yyyy-MM-dd")
        self.date_end.setObjectName("DateEdit")

        date_grid.addWidget(lbl_end, 0, 1)
        date_grid.addWidget(self.date_end, 1, 1)

        layout.addLayout(date_grid)
        layout.addSpacing(10)

        # 3. 独立操作区 (按钮和状态)
        action_layout = QHBoxLayout()
        action_layout.setSpacing(10)

        self.btn_export_single = QPushButton("导出该任务")
        self.btn_export_single.setObjectName("SingleExportButton")
        self.btn_export_single.setCursor(Qt.CursorShape.PointingHandCursor)
        self.btn_export_single.setFixedHeight(30)

        self.lbl_status_single = QLabel("就绪")
        self.lbl_status_single.setStyleSheet("font-size: 9pt; color: #86868B; background: transparent;")
        self.lbl_status_single.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)

        action_layout.addWidget(self.btn_export_single)
        action_layout.addWidget(self.lbl_status_single)

        layout.addLayout(action_layout)

    def get_dates(self) -> Dict[str, str]:
        """返回任务的开始和结束日期字符串"""
        return {
            "start": get_ui_date_string(self.date_start),
            "end": get_ui_date_string(self.date_end)
        }

    def set_status(self, status: str, color: str):
        """设置卡片底部状态文本和颜色"""
        self.lbl_status_single.setStyleSheet(
            f"font-size: 9pt; font-weight: 500; color: {color}; background: transparent;")
        self.lbl_status_single.setText(status)

    def set_buttons_enabled(self, enabled: bool):
        """启用或禁用卡片上的按钮"""
        self.btn_export_single.setEnabled(enabled)


# --- 批量导出页面类 ---

class BatchExportPage(QWidget):
    PAGE_NAME = "批量数据导出"

    def __init__(self, parent=None):
        super().__init__(parent)

        self.worker: Optional[BatchExporterWorker] = None
        self.thread: Optional[QThread] = None
        self.task_cards: Dict[str, TaskInputCard] = {}
        self.current_task_key: Optional[str] = None

        self.export_folder = os.path.join(os.getcwd(), 'Batch_Exported_Data')
        os.makedirs(self.export_folder, exist_ok=True)
        self.is_batch_mode = False

        self._setup_ui()
        self.apply_styles()
        self._bind_card_signals()

    def _create_task_cards(self):
        """创建所有任务输入卡片"""
        today = QDate.currentDate()
        # --- 新增：计算昨日的日期 ---
        yesterday = today.addDays(-1)
        # -----------------------------

        self.task_cards = {
            # 将默认日期都设置为昨日
            "violation": TaskInputCard("风神违规数据", "violation", yesterday, yesterday),
            "schedule": TaskInputCard("骑手排班数据", "schedule", yesterday, yesterday),
            "attendance": TaskInputCard("骑手考勤数据", "attendance", yesterday, yesterday),
            "daily_detail": TaskInputCard("骑手每日详情", "daily_detail", yesterday, yesterday),
        }
        return self.task_cards

    def _bind_card_signals(self):
        """绑定每个 TaskInputCard 上的独立导出按钮到统一的槽函数"""
        for task_key, card in self.task_cards.items():
            card.btn_export_single.clicked.connect(
                lambda checked, key=task_key: self.start_single_export(key)
            )

    def _setup_ui(self):
        # 1. 外部容器和居中布局
        center_layout = QVBoxLayout(self)
        center_layout.setContentsMargins(0, 0, 0, 0)
        center_layout.addStretch()

        # 页面主体容器 (用于投影和圆角)
        self.outer_container = QFrame()
        self.outer_container.setObjectName("PageContainer")
        self.outer_container.setFixedWidth(720)

        # 页面总布局 (ScrollArea + BottomBar)
        final_layout = QVBoxLayout(self.outer_container)
        final_layout.setContentsMargins(0, 0, 0, 0)

        # 2. 滚动区域 (ScrollArea)
        self.scroll_area = QScrollArea()
        self.scroll_area.setWidgetResizable(True)
        self.scroll_area.setFrameShape(QFrame.Shape.NoFrame)
        self.scroll_area.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        self.scroll_area.setObjectName("PageScrollArea")

        # 2.1 滚动区域内的内容 Widget
        self.main_content_widget = QWidget()
        self.main_content_layout = QVBoxLayout(self.main_content_widget)
        self.main_content_layout.setContentsMargins(50, 50, 50, 20)
        self.main_content_layout.setSpacing(25)

        # 标题
        title_box = QHBoxLayout()
        title_icon = QLabel("🚀")
        title_icon.setStyleSheet("font-size: 28pt; background: transparent;")
        title = QLabel("风神数据批量导出工具")
        title.setObjectName("page_title")
        title_box.addStretch()
        title_box.addWidget(title_icon)
        title_box.addWidget(title)
        title_box.addStretch()
        self.main_content_layout.addLayout(title_box)
        self.main_content_layout.addSpacing(10)

        # 团队 ID 输入
        input_layout = QVBoxLayout()
        input_layout.setSpacing(10)
        lbl_team_ids = QLabel(f"🛵 团队 ID (多个用逗号分隔，例如: 12345,67890)")
        lbl_team_ids.setObjectName("input_label")
        self.entry_team_ids = QLineEdit()
        self.entry_team_ids.setPlaceholderText("请输入要导出的团队 ID 列表...")
        self.entry_team_ids.setText(DEFAULT_TEAM_IDS_STR)
        lbl_export_folder = QLabel(f"📂 导出目录: {self.export_folder}",
                                   styleSheet="font-size: 10pt; color: #86868B; background: transparent;")
        input_layout.addWidget(lbl_team_ids)
        input_layout.addWidget(self.entry_team_ids)
        input_layout.addWidget(lbl_export_folder)
        self.main_content_layout.addLayout(input_layout)
        self.main_content_layout.addSpacing(15)

        # 任务卡片列表标签
        lbl_task_selector = QLabel("📅 任务和日期选择 (可独立导出或批量导出)",
                                   styleSheet="font-size: 10pt; font-weight: 500; color: #86868B; background: transparent;")
        self.main_content_layout.addWidget(lbl_task_selector)

        # 任务卡片布局
        self.task_cards_layout = QVBoxLayout()
        self.task_cards_layout.setContentsMargins(0, 0, 0, 0)
        self.task_cards_layout.setSpacing(15)
        self.task_cards = self._create_task_cards()
        for key in self.task_cards:
            self.task_cards_layout.addWidget(self.task_cards[key])
        self.task_cards_layout.addStretch()
        self.main_content_layout.addLayout(self.task_cards_layout)

        self.scroll_area.setWidget(self.main_content_widget)
        final_layout.addWidget(self.scroll_area)

        # 3. 底部固定操作栏 (BottomBar)
        self.bottom_bar = QFrame()
        self.bottom_bar.setObjectName("BottomBar")
        self.bottom_bar.setContentsMargins(50, 15, 50, 25)
        bottom_layout = QVBoxLayout(self.bottom_bar)
        bottom_layout.setContentsMargins(0, 0, 0, 0)

        # 按钮
        bottom_btn_layout = QHBoxLayout()
        bottom_btn_layout.setSpacing(15)

        self.btn_start_batch = QPushButton("✅ 开始批量导出 (所有任务)")
        self.btn_start_batch.setObjectName("StartButton")
        self.btn_start_batch.setCursor(Qt.CursorShape.PointingHandCursor)
        self.btn_start_batch.setFixedHeight(45)
        self.btn_start_batch.clicked.connect(self.start_batch_export)

        self.btn_stop = QPushButton("🛑 停止")
        self.btn_stop.setObjectName("StopButton")
        self.btn_stop.setCursor(Qt.CursorShape.PointingHandCursor)
        self.btn_stop.setFixedHeight(45)
        self.btn_stop.clicked.connect(self.stop_worker)
        self.btn_stop.setEnabled(False)
        self.btn_stop.setFixedWidth(100)

        bottom_btn_layout.addWidget(self.btn_start_batch)
        bottom_btn_layout.addWidget(self.btn_stop)

        # 进度条和状态标签
        status_layout = QVBoxLayout()
        status_layout.setSpacing(5)
        self.lbl_status = QLabel("准备就绪")
        self.lbl_status.setObjectName("StatusLabel")
        self.lbl_status.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.progress_bar = QProgressBar()
        self.progress_bar.setValue(0)
        self.progress_bar.setTextVisible(False)
        self.progress_bar.setFixedHeight(6)

        status_layout.addWidget(self.lbl_status)
        status_layout.addWidget(self.progress_bar)

        # 打开文件夹按钮
        link_layout = QHBoxLayout()
        self.btn_open = QPushButton("📂 打开导出文件夹")
        self.btn_open.setObjectName("OpenButton")
        self.btn_open.setCursor(Qt.CursorShape.PointingHandCursor)
        self.btn_open.setFlat(True)
        self.btn_open.clicked.connect(self.open_output_directory)
        link_layout.addStretch()
        link_layout.addWidget(self.btn_open)
        link_layout.addStretch()

        bottom_layout.addLayout(bottom_btn_layout)
        bottom_layout.addSpacing(10)
        bottom_layout.addLayout(status_layout)
        bottom_layout.addSpacing(15)
        bottom_layout.addLayout(link_layout)

        final_layout.addWidget(self.bottom_bar)

        # 4. 应用投影并居中
        shadow = QGraphicsDropShadowEffect(self)
        shadow.setBlurRadius(40)
        shadow.setXOffset(0)
        shadow.setYOffset(10)
        shadow.setColor(QColor(0, 0, 0, 20))
        self.outer_container.setGraphicsEffect(shadow)

        center_layout.addWidget(self.outer_container, alignment=Qt.AlignmentFlag.AlignCenter)
        center_layout.addStretch()

    def apply_styles(self):
        """应用 Apple 风格的现代 UI 样式"""

        BG_COLOR = "#F5F5F7"
        CARD_COLOR = "#FFFFFF"
        TEXT_PRIMARY = "#1D1D1F"
        TEXT_SECONDARY = "#86868B"
        ACCENT_BLUE = "#0071E3"
        ACCENT_RED = "#FF3B30"
        INPUT_BG = "#F5F5F7"
        SCROLL_AREA_BG = "#FFFFFF"
        ACCENT_GREEN = "#34C759"  # 用于成功的颜色

        self.setStyleSheet(f"""
            /* 1. 全局和外部容器 */
            QWidget {{
                background-color: {BG_COLOR}; 
                color: {TEXT_PRIMARY};
                font-family: "SF Pro Text", "Helvetica Neue", "Microsoft YaHei", sans-serif;
            }}
            #PageContainer {{
                background-color: {CARD_COLOR};
                border-radius: 20px;
            }}
            QLabel {{ background-color: transparent; }}

            /* 2. 底部固定操作栏样式 */
            #BottomBar {{
                background-color: {CARD_COLOR}; 
                border-top: 1px solid #E5E5EA; 
            }}

            /* 3. 滚动区域样式 (现在是整个主体) */
            #PageScrollArea {{
                border: none;
                background-color: {SCROLL_AREA_BG};
            }}
            QScrollBar:vertical {{
                border: none;
                background: #E5E5EA;
                width: 8px;
                margin: 0px;
                border-radius: 4px;
            }}
            QScrollBar::handle:vertical {{
                background: #C7C7CC;
                min-height: 20px;
                border-radius: 4px;
            }}

            /* 4. 任务输入卡片 */
            #TaskCard {{
                background-color: {CARD_COLOR}; 
                border-radius: 12px;
                box-shadow: 0 4px 6px rgba(0, 0, 0, 0.05); 
                border: 1px solid #E5E5EA;
            }}

            /* 5. 日期选择器 (美化重点) */
            QDateEdit {{
                border: 1px solid #D1D1D6;
                border-radius: 8px;
                padding: 6px 10px; /* 内部填充 */
                background-color: {INPUT_BG}; 
                color: {TEXT_PRIMARY};
                font-size: 11pt;
                text-shadow: 0 1px 0 rgba(255, 255, 255, 0.5);
            }}
            QDateEdit:focus {{
                border: 1px solid {ACCENT_BLUE};
                background-color: {CARD_COLOR}; /* 聚焦时变亮 */
            }}
            /* 下拉箭头样式 */
            QDateEdit::drop-down {{
                subcontrol-origin: padding;
                subcontrol-position: center right;
                width: 20px;
                border-left: 1px solid #D1D1D6; /* 分隔线 */
                padding: 0 5px;
            }}

            /* ========================================================= */
            /* 9. 弹出式日历 (QCalendarWidget) 样式增强 (重点修改部分) */
            /* ========================================================= */
            QCalendarWidget {{
                background-color: {CARD_COLOR};
                border: 1px solid #C7C7CC; /* 边框 */
                border-radius: 12px; /* 增加圆角 */
                padding: 5px;
            }}

            /* 导航栏 (显示月份和年份的顶部区域) */
            QCalendarWidget QWidget#qt_calendar_navigationbar {{
                background-color: {CARD_COLOR};
                border-bottom: 1px solid #E5E5EA; /* 分隔线 */
                padding-bottom: 5px;
            }}

            /* 导航按钮 (左右箭头) */
            QCalendarWidget QToolButton {{
                border: none;
                icon-size: 16px;
                margin: 0px 5px;
                padding: 5px;
                border-radius: 6px;
                background-color: {CARD_COLOR};
            }}
            QCalendarWidget QToolButton:hover {{
                background-color: {INPUT_BG}; /* 悬停效果 */
            }}
            QCalendarWidget QToolButton::menu-indicator {{
                /* 移除菜单下拉指示器 */
                image: none;
            }}

            /* 导航标签 (月份和年份文本) */
            QCalendarWidget QAbstractItemView:enabled {{
                background-color: {CARD_COLOR}; 
                font-size: 10pt;
            }}

            /* 日期主体部分 (显示日期的网格) */
            QCalendarWidget QAbstractItemView {{
                font-size: 10pt;
                padding-top: 5px; /* 增加与导航栏的距离 */
            }}

            /* 星期标题 (周一、周二...) */
            QCalendarWidget QAbstractItemView::item:text:nth-child(7n-1), /* 周六 */
            QCalendarWidget QAbstractItemView::item:text:nth-child(7n)   /* 周日 */
            {{
                color: {ACCENT_RED}; /* 周末红色 */
            }}

            /* 未被选中的普通日期和悬停效果 */
            QCalendarWidget QAbstractItemView::item:!selected:hover {{
                background-color: {INPUT_BG}; /* 日期悬停效果 */
                border-radius: 6px;
            }}

            /* 禁用日期（上个月/下个月的日期） */
            QCalendarWidget QAbstractItemView::item:!enabled {{
                color: #C7C7CC; 
            }}

            /* 选中日期 (当前选定的日期) */
            QCalendarWidget QAbstractItemView::item:selected {{
                background-color: {ACCENT_BLUE}; 
                color: white; 
                border-radius: 6px;
            }}

            /* 今天日期的特殊标记（通常由QCalendarWidget自动实现，但可以通过此项确保颜色统一） */
            /* 注意: QCalendarWidget的 'today' 状态可能需要更复杂的代理样式，此处主要靠选中状态和背景色处理 */


            /* 6. 输入框 */
            QLineEdit {{
                border: none;
                border-radius: 10px;
                padding: 12px 16px;
                background-color: {INPUT_BG}; 
                color: {TEXT_PRIMARY};
                font-size: 11pt;
            }}

            /* 7. 按钮样式 */
            #StartButton {{ background-color: {ACCENT_BLUE}; color: white; border-radius: 18px; padding: 10px 24px; font-weight: 600; }}
            #StopButton {{ background-color: rgba(255, 59, 48, 0.1); color: {ACCENT_RED}; border-radius: 18px; padding: 10px 24px; font-weight: 600; }}
            #OpenButton {{ background-color: {CARD_COLOR}; color: {TEXT_PRIMARY}; border-radius: 18px; padding: 10px 24px; font-weight: 600; }}

            /* 卡片内部的独立导出按钮 */
            #SingleExportButton {{
                background-color: #E5E5EA;
                color: {TEXT_PRIMARY};
                border-radius: 15px;
                padding: 6px 15px;
                font-weight: 500;
                font-size: 10pt;
            }}
            #SingleExportButton:hover {{ background-color: #D1D1D6; }}

            /* 8. 状态和进度条 */
            #StatusLabel {{ font-size: 10pt; color: {TEXT_SECONDARY}; }}
            QProgressBar {{ border-radius: 3px; background-color: #E5E5EA; height: 6px; }}
            QProgressBar::chunk {{ background-color: {ACCENT_BLUE}; border-radius: 3px; }}

        """)

    # --- 辅助方法：收集 UI 数据 ---
    def _collect_task_date_params(self) -> Dict[str, Dict[str, str]]:
        """从所有卡片中收集日期参数"""
        date_params = {}
        for task_key, card in self.task_cards.items():
            date_params[task_key] = card.get_dates()
        return date_params

    def _collect_active_task_keys(self) -> List[str]:
        """收集所有任务的键名 (用于批量模式)"""
        return list(self.task_cards.keys())

    def _validate_inputs(self):
        """校验团队 ID，并返回列表或 None"""
        team_id_str = self.entry_team_ids.text().strip()
        try:
            team_ids = [int(id.strip()) for id in team_id_str.replace(' ', ',').split(',') if id.strip().isdigit()]
            if not team_ids:
                QMessageBox.warning(self, "参数缺失", "请输入有效的团队 ID 列表。")
                return None
            return team_ids
        except Exception:
            QMessageBox.critical(self, "参数错误", "无法解析团队 ID，请确保输入格式正确。")
            return None

    def _start_worker_thread(self, mode: str, task_key: str = None):
        """启动 Worker 线程的通用逻辑"""
        team_ids = self._validate_inputs()
        if team_ids is None:
            return

        # 1. 检查是否有任务正在运行
        if self.thread is not None and self.thread.isRunning():
            reply = QMessageBox.question(
                self, "任务运行中",
                "已有任务在运行中，是否停止当前任务并开始新的任务？",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
            )
            if reply == QMessageBox.StandardButton.Yes:
                self.stop_worker()
                # 等待线程停止
                for i in range(10):
                    if self.thread is None or not self.thread.isRunning():
                        break
                    QThread.msleep(100)
            else:
                return

        # 2. 准备参数
        date_params = self._collect_task_date_params()
        task_keys = self._collect_active_task_keys()

        # 3. 创建新的线程和 Worker
        self.is_batch_mode = (mode == "batch")
        self.current_task_key = task_key

        # 清理旧的线程和 Worker
        if self.thread and self.thread.isRunning():
            self.worker.stop()
            self.thread.quit()
            self.thread.wait(1000)

        self.thread = QThread()
        self.worker = BatchExporterWorker(self.export_folder)

        # 4. 设置 Worker 的参数
        self.worker.set_export_parameters(team_ids, date_params, task_keys)
        self.worker.moveToThread(self.thread)

        # 5. ✅ 连接 Worker 信号到主界面的槽函数（使用真实 Worker 的信号）
        self.worker.error_occurred.connect(self._handle_worker_error)
        self.worker.progress_update.connect(self.update_progress)

        if mode == "batch":
            self.worker.finished_batch.connect(self.thread_finished_batch)
            self.progress_bar.setMaximum(len(self.task_cards))
        else:
            self.worker.finished_single.connect(self.thread_finished_single)
            self.progress_bar.setMaximum(1)
            self.progress_bar.setValue(0)

        # 6. 连接线程启动和清理信号
        if mode == "batch":
            self.thread.started.connect(self.worker.run_batch)
        else:
            self.thread.started.connect(lambda: self.worker.run_single(task_key))

        self.worker.finished_single.connect(self.thread.quit)
        self.worker.finished_batch.connect(self.thread.quit)
        self.thread.finished.connect(self._on_thread_finished)

        # 7. 启动线程
        self._set_ui_running_state(True, task_key=task_key)
        self.thread.start()

    def _on_thread_finished(self):
        """线程完成后的清理"""
        if self.thread:
            self.thread.deleteLater()
            self.thread = None
        if self.worker:
            self.worker.deleteLater()
            self.worker = None

    @Slot()
    def start_batch_export(self):
        """启动所有任务的批量导出"""
        self._start_worker_thread(mode="batch")

    @Slot(str)
    def start_single_export(self, task_key: str):
        """启动单个任务的导出"""
        self._start_worker_thread(mode="single", task_key=task_key)

    @Slot(str)
    def _handle_worker_error(self, message: str):
        """处理线程返回的错误 (只接收 message)"""
        print(f"Error received: {message}")

        if self.is_batch_mode:
            # 批量模式下，显示错误但继续执行
            self.lbl_status.setStyleSheet("color: #E74C3C; font-weight: bold;")
            self.lbl_status.setText(f"批量任务出错: {message[:50]}...")
        elif self.current_task_key:
            # 单任务模式，更新卡片状态
            card = self.task_cards.get(self.current_task_key)
            if card:
                card.set_status("❌ 失败", "#E74C3C")
            self.lbl_status.setText(f"❌ 任务 [{self.current_task_key}] 失败")

        # 显示错误对话框
        QMessageBox.critical(self, "任务失败", message)

    @Slot()
    def stop_worker(self):
        """停止当前正在运行的 Worker"""
        if self.worker and self.thread and self.thread.isRunning():
            self.worker.stop()
            self.lbl_status.setText("🛑 正在尝试安全停止任务...")
            self.btn_stop.setEnabled(False)

    def _set_ui_running_state(self, is_running: bool, task_key: str = None):
        """设置整体和卡片的 UI 状态"""
        self.btn_start_batch.setEnabled(not is_running)
        self.btn_stop.setEnabled(is_running)

        for key, card in self.task_cards.items():
            card.set_buttons_enabled(not is_running)

            if is_running and not self.is_batch_mode and key == task_key:
                card.set_status("进行中...", "#0071E3")
                card.set_buttons_enabled(False)
            elif not is_running and not self.is_batch_mode:
                # 单任务结束后，重置状态条
                self.lbl_status.setText("准备就绪")
                self.progress_bar.setValue(0)

        if is_running and self.is_batch_mode:
            self.lbl_status.setText("批量任务启动中...")

    @Slot(int, str)
    def update_progress(self, index, message):
        """接收 Worker 发出的进度信息"""
        self.progress_bar.setValue(index)
        if self.is_batch_mode:
            self.lbl_status.setText(f"批量任务进行中 [{index}/{len(self.task_cards)}]: {message}")
        else:
            self.lbl_status.setText(f"单任务进行中: {message}")

    @Slot(str, str, str)
    def thread_finished_single(self, task_key: str, status: str, file_name: str):
        """处理单个任务完成"""
        self._set_ui_running_state(False)
        card = self.task_cards.get(task_key)

        if card:
            if status == "成功":
                card.set_status(f"✅ 完成: {file_name}", "#2ECC71")
                self.lbl_status.setText(f"单任务 [{task_key}] 导出成功: {file_name}")
            else:
                card.set_status("❌ 失败", "#E74C3C")

    @Slot(str, str)
    def thread_finished_batch(self, status, output_dir):
        """处理批量任务完成"""
        self._set_ui_running_state(False)

        if status == "成功":
            self.lbl_status.setStyleSheet("color: #2ECC71; font-weight: bold;")
            self.lbl_status.setText(f"✅ 所有任务成功完成！文件保存在: {os.path.abspath(output_dir)}")
            self.progress_bar.setValue(self.progress_bar.maximum())
        elif status == "取消":
            self.lbl_status.setStyleSheet("color: #F39C12; font-weight: bold;")
            self.lbl_status.setText("⚠️ 任务被手动取消。")
            self.progress_bar.setValue(0)
        else:
            self.lbl_status.setStyleSheet("color: #E74C3C; font-weight: bold;")
            self.lbl_status.setText(f"❌ 批量任务失败，部分文件可能已下载")
            self.progress_bar.setValue(0)

        # 批量任务结束后，重置所有卡片状态为就绪
        for card in self.task_cards.values():
            card.set_status("就绪", "#86868B")

    @Slot()
    def open_output_directory(self):
        """打开导出文件所在的目录"""
        directory = self.export_folder

        if os.path.exists(directory):
            if sys.platform == "win32":
                os.startfile(directory)
            elif sys.platform == "darwin":
                os.system(f'open "{directory}"')
            else:
                os.system(f'xdg-open "{directory}"')
        else:
            QMessageBox.warning(self, "文件未找到", f"导出目录不存在: {directory}")




class CrawlerPage(QWidget):
    # 页面名称，方便在主窗口中识别
    PAGE_NAME = "风神离职数据"

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


# --- 2. 欢迎页面 (HomePage) ---
class HomePage(QWidget):
    PAGE_NAME = "首页"
    # 新增信号：用于通知 MainWindow 切换到指定名称的页面
    navigate_to_page = Signal(str)

    def __init__(self, parent=None):
        super().__init__(parent)
        self._setup_ui()
        self.apply_styles()

    def apply_styles(self):
        # 统一的 Apple 风格配色方案
        BG_COLOR = "#F5F5F7"  # 窗口大背景 (高级灰)
        CARD_COLOR = "#FFFFFF"  # 卡片纯白
        TEXT_PRIMARY = "#1D1D1F"  # 主要文字色 (接近黑)
        TEXT_SECONDARY = "#86868B"  # 次要文字色 (柔和灰)
        ACCENT_BLUE = "#0071E3"  # 爬取卡片标题色 (系统蓝)
        ACCENT_GREEN = "#34C759"  # 合并卡片标题色 (系统绿)
        HOVER_COLOR = "#FAFAFA"  # 柔和的悬停背景色

        self.setStyleSheet(f"""
            /* --- 1. 全局基础样式 --- */
            QWidget {{ 
                background-color: {BG_COLOR}; 
                font-family: "SF Pro Text", "Helvetica Neue", "Microsoft YaHei", sans-serif;
                color: {TEXT_PRIMARY};
            }}

            /* --- 2. 【关键修复】强制所有 QLabel 背景透明 --- */
            QLabel {{
                background-color: transparent;
                /* 确保所有标签文字颜色基于上下文，这里使用默认 PRIMARY */
                color: {TEXT_PRIMARY};
            }}

            /* --- 3. 欢迎标题和描述 --- */
            #WelcomeTitle {{ 
                color: {TEXT_PRIMARY}; 
                font-size: 30pt; /* 略微缩小，更优雅 */
                font-weight: 600; /* Semibold */
                padding-bottom: 10px;
            }}
            #AppDesc {{ 
                color: {TEXT_SECONDARY}; 
                font-size: 12pt; 
                margin-bottom: 40px; 
            }}

            /* ==================================== */
            /* 优化的卡片按钮样式 (.FeatureButton) */
            /* ==================================== */
            .FeatureButton {{
                background-color: {CARD_COLOR}; /* 纯白背景 */
                border-radius: 16px; /* 增大圆角，更柔和 */
                padding: 30px; /* 增大内边距，增加呼吸感 */
                text-align: left;
                border: none; /* 移除边框 */

                /* 阴影效果：模仿 Apple UI 的柔和投影 */
                box-shadow: 0 8px 30px rgba(0, 0, 0, 0.04); 
            }}

            .FeatureButton:hover {{
                background-color: {HOVER_COLOR}; 
                /* 悬停时阴影加重，模拟抬升 */
                box-shadow: 0 12px 35px rgba(0, 0, 0, 0.08); 
            }}

            .FeatureButton:pressed {{
                background-color: #EFEFEF;
                box-shadow: none; /* 按下时移除阴影，模拟下沉 */
            }}

            /* --- 标题和描述文字 --- */

            /* 爬取卡片标题 (蓝色) */
            #FeatureTitle_Crawler {{ 
                color: {ACCENT_BLUE}; 
                font-size: 16pt; 
                font-weight: 600; /* Semibold */
                /* 注意：这里移除了 !important */
            }}

            /* 合并卡片标题 (绿色) */
            #FeatureTitle_Merge {{ 
                color: {ACCENT_GREEN}; 
                font-size: 16pt; 
                font-weight: 600;
                /* 注意：这里移除了 !important */
            }}

            /* 描述文字 (统一深色) */
            .FeatureDesc {{
                color: {TEXT_SECONDARY}; /* 使用次要文字色，不使用 !important */
                font-size: 10pt;
            }}
        """)

    def _setup_ui(self):
        main_layout = QVBoxLayout(self)
        # 增大外围留白，更像网页
        main_layout.setContentsMargins(100, 80, 100, 80)
        main_layout.setSpacing(60)  # 增大间距

        # 欢迎信息
        welcome_layout = QVBoxLayout()
        welcome_layout.setSpacing(10)  # 标题和描述的间距

        title = QLabel("欢迎使用自动化工具箱")
        title.setObjectName("WelcomeTitle")
        title.setAlignment(Qt.AlignmentFlag.AlignHCenter)
        welcome_layout.addWidget(title)

        desc = QLabel("本工具集成数据爬取与本地处理功能，助您高效完成日常数据工作。")
        desc.setObjectName("AppDesc")
        desc.setAlignment(Qt.AlignmentFlag.AlignHCenter)
        welcome_layout.addWidget(desc)

        main_layout.addLayout(welcome_layout)

        # 功能介绍
        features_layout = QHBoxLayout()
        features_layout.setSpacing(30)  # 卡片之间的间距增大

        # 保持原始逻辑，但假设 CrawlerPage 和 MergePage 已定义
        # (如果您在 PyCharm/VSC 编辑器中，需要确保这两个类已导入)
        class CrawlerPage: PAGE_NAME = "风神离职数据"

        class MergePage: PAGE_NAME = "B端数据处理"

        crawler_page_name = CrawlerPage.PAGE_NAME
        features_layout.addWidget(self._create_feature_card(
            "💻 数据爬取与导出",
            "一键获取风神骑手离职历史。",
            # color 参数不再用于QSS，但保留函数签名，以兼容旧的调用
            "#007AFF",
            target_page=crawler_page_name
        ))

        merge_page_name = MergePage.PAGE_NAME
        features_layout.addWidget(self._create_feature_card(
            "🗂️ 本地文件处理",
            "轻松合并【有效商户明细】和【商智核/超抢手】两份 CSV 文件。",
            # color 参数不再用于QSS，但保留函数签名，以兼容旧的调用
            "#34C759",
            target_page=merge_page_name
        ))

        main_layout.addLayout(features_layout)
        main_layout.addStretch()

    def _create_feature_card(self, title: str, description: str, color: str, target_page: str) -> QWidget:
        # 使用 QPushButton 来实现卡片的点击效果
        button = QPushButton()
        button.setObjectName("FeatureButton")
        # 移除默认的按钮边框
        button.setFlat(True)
        button.setCursor(Qt.CursorShape.PointingHandCursor)  # 增加指针
        # 设置SizePolicy使其可以横向扩展
        button.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Minimum)
        # 点击后发送信号，携带目标页面的名称
        button.clicked.connect(lambda: self.navigate_to_page.emit(target_page))

        # 将卡片内容布局放在按钮内部
        layout = QVBoxLayout(button)
        layout.setSpacing(8)  # 略微减小标题和描述的间距，让它们看起来更像一组

        # 标题标签
        title_label = QLabel(title)

        # 💡 保持原始逻辑：根据目标页面设置独特的ID
        # 假设这两个类已定义
        class CrawlerPage:
            PAGE_NAME = "数据爬取与导出"

        class MergePage:
            PAGE_NAME = "本地文件处理"

        if target_page == CrawlerPage.PAGE_NAME:
            title_label.setObjectName("FeatureTitle_Crawler")
        elif target_page == MergePage.PAGE_NAME:
            title_label.setObjectName("FeatureTitle_Merge")
        else:
            title_label.setObjectName("FeatureTitle_Default")

        layout.addWidget(title_label)

        # 描述标签
        desc_label = QLabel(description)
        desc_label.setObjectName("FeatureDesc")  # 使用通用的类选择器
        desc_label.setWordWrap(True)
        layout.addWidget(desc_label)

        # 必须将 layout 设置到按钮上
        button.setLayout(layout)
        return button

        # 描述标签
        desc_label = QLabel(description)
        desc_label.setObjectName("FeatureDesc")  # 使用通用的类选择器
        desc_label.setWordWrap(True)
        layout.addWidget(desc_label)

        # 必须将 layout 设置到按钮上
        button.setLayout(layout)
        return button

#
# # --- 3. 侧边栏 (SidebarWidget) ---
#
# class SidebarWidget(QWidget):
#     page_switched = Signal(int)
#
#     def __init__(self, parent=None):
#         super().__init__(parent)
#
#         self.current_button = None
#
#         self.setFixedWidth(60)
#         self._setup_ui()
#         self.apply_styles()
#
#     def apply_styles(self):
#         """优化侧边栏样式，增加活动指示条和悬停效果，并移除点击时的默认效果"""
#         SIDEBAR_BG = "#2C3E50"  # 深蓝色背景
#         HOVER_BG = "#3A5168"  # 悬停颜色
#         ACTIVE_COLOR = "#1ABC9C"  # 绿色活动指示条
#
#         self.setStyleSheet(f"""
#             QWidget {{
#                 background-color: {SIDEBAR_BG};
#             }}
#             /* 导航按钮的基样式 */
#             #SidebarButton {{
#                 border: none;
#                 background-color: transparent;
#                 padding: 10px 0;
#                 margin: 5px 0;
#                 color: #ECF0F1;
#                 transition: background-color 0.2s, border-left 0.2s; /* 增加平滑过渡 */
#             }}
#             #SidebarButton QIcon {{
#                 color: #ECF0F1;
#                 qproperty-iconSize: 24px;
#             }}
#
#             /* 悬停效果：改变背景色 */
#             #SidebarButton:hover {{
#                 background-color: {HOVER_BG};
#             }}
#
#             /* 按下效果：移除默认的按下阴影/边框，保持悬停色或透明 */
#             #SidebarButton:pressed {{
#                 background-color: {HOVER_BG}; /* 点击时不改变颜色，保持与悬停色一致 */
#                 border: none; /* 移除点击时可能出现的默认边框 */
#                 padding: 10px 0; /* 保持padding，防止“下沉”效果 */
#             }}
#
#             /* 活动/选中效果：改变背景色并添加左侧指示条 */
#             #SidebarButton[active="true"] {{
#                 border-left: 4px solid {ACTIVE_COLOR}; /* 关键：左侧指示条 */
#
#             }}
#
#             /* 菜单和设置按钮 (非导航按钮) */
#             QPushButton#SidebarButton:!checkable {{
#                 /* 确保菜单和设置按钮不被 active 状态影响 */
#                 border-left: none;
#                 padding-left: 0;
#             }}
#         """)
#
#     def _setup_ui(self):
#         layout = QVBoxLayout(self)
#         layout.setContentsMargins(0, 10, 0, 10)
#         layout.setSpacing(5)
#         layout.setAlignment(Qt.AlignmentFlag.AlignTop | Qt.AlignmentFlag.AlignCenter)
#
#         # --- 菜单图标 (假设，图片中的三条线) ---
#         btn_menu = QPushButton(QIcon("./icons/单据.png"), "")
#         btn_menu.setObjectName("SidebarButton")
#         btn_menu.setIconSize(QSize(24, 24))
#         layout.addWidget(btn_menu)
#         layout.addWidget(self.create_separator(1))
#
#         # --- 导航按钮 ---
#
#         # 1. 主页按钮 (索引 0)
#         self.btn_home = self._create_nav_button("./icons/仓库.png", 0)
#         layout.addWidget(self.btn_home)
#
#         # 2. 合并功能按钮 (索引 1)
#         self.btn_merge = self._create_nav_button("./icons/看板.png", 1)
#         layout.addWidget(self.btn_merge)
#
#         # 3. 占位符按钮 (索引 2)
#         self.btn_placeholder = self._create_nav_button("./icons/指标.png", 2)
#         layout.addWidget(self.btn_placeholder)
#
#         self.btn_placeholder = self._create_nav_button("./icons/云爬虫.png", 4)
#         layout.addWidget(self.btn_placeholder)
#
#         layout.addStretch()
#         layout.addWidget(self.create_separator(2))
#
#         # --- 底部设置按钮 (模拟图片中的齿轮) ---
#         # btn_settings = QPushButton(QIcon("./icons/设置_管理.png"), "")
#         self.btn_placeholder = self._create_nav_button("./icons/设置_管理.png", 3)
#         layout.addWidget(self.btn_placeholder)
#
#
#
#         # btn_settings.setObjectName("SidebarButton")
#         # btn_settings.setIconSize(QSize(24, 24))
#         # layout.addWidget(btn_settings)
#
#         # 初始设置选中状态
#         self.set_active_button(self.btn_home)
    # def _create_nav_button(self, icon_path, index):
    #     """创建并连接导航按钮"""
    #     button = QPushButton(QIcon(icon_path), "")
    #     button.setObjectName("SidebarButton")
    #     button.setIconSize(QSize(24, 24))
    #     button.setCheckable(True)
    #     button.clicked.connect(lambda: self.on_nav_clicked(button, index))
    #     return button
    #
    # def create_separator(self, height):
    #     line = QLabel()
    #     line.setFixedHeight(height)
    #     line.setStyleSheet("background-color: #4A6075;")
    #     return line
    #
    # def on_nav_clicked(self, clicked_button, index):
    #     """处理导航按钮点击事件"""
    #     self.set_active_button(clicked_button)
    #     self.page_switched.emit(index)
    #
    # def set_active_button(self, button):
    #     """设置按钮的活动状态并清除前一个按钮的状态"""
    #     if self.current_button and self.current_button != button:
    #         self.current_button.setProperty("active", "false")
    #         self.current_button.style().polish(self.current_button)  # 强制刷新样式
    #
    #     button.setProperty("active", "true")
    #     button.style().polish(button)  # 强制刷新样式
    #     self.current_button = button


# --- 5. 主窗口：使用侧边栏布局 (MainWindow) ---

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        # 1. 设置图标路径
        # 注意：建议使用 .ico 文件以获得最佳兼容性
        icon_path = "./icons/图标.png"

        # 2. 设置窗口标题和尺寸
        self.setWindowTitle("数据自动化工具箱 v1.0")
        self.setMinimumSize(1000, 700)

        # 3. 设置窗口图标 (新增步骤)
        # 必须在 self.apply_styles() 之前或之后调用
        self.setWindowIcon(QIcon(icon_path))

        # 4. 应用样式
        self.apply_styles()

        # 5. 设置 UI 结构 (页面切换器和导航)
        self._setup_ui()

    def apply_styles(self):
        # 统一的配色方案
        PRIMARY_COLOR = "#007AFF"
        BACKGROUND_COLOR = "#F0F4F7"

        self.setStyleSheet(f"""
            QMainWindow {{ background-color: {BACKGROUND_COLOR}; }}

            /* 侧边导航栏 */
            #SideBar {{
                background-color: #2C3E50; /* 深色背景 */
                border-right: 1px solid #34495E;
            }}

            /* 导航按钮 */
            .NavButton {{
                background-color: transparent;
                color: #ECF0F1; /* 浅色文本 */
                border: none;
                padding: 15px 15px;
                text-align: left;
                font-size: 11pt;
                font-weight: 500;
                border-left: 5px solid transparent;
                transition: all 0.2s;
            }}
            .NavButton:hover {{
                background-color: #34495E;
                border-left-color: #5D6D7E;
            }}
            .NavButton.active {{
                background-color: #34495E;
                color: {PRIMARY_COLOR}; /* 选中高亮色 */
                border-left-color: {PRIMARY_COLOR};
                font-weight: bold;
            }}
        """)

    def _setup_ui(self):
        central_widget = QWidget()
        main_layout = QHBoxLayout(central_widget)
        main_layout.setContentsMargins(0, 0, 0, 0)
        main_layout.setSpacing(0)

        # 1. 侧边导航栏 (SideBar)
        self.sidebar = QWidget()
        self.sidebar.setObjectName("SideBar")
        self.sidebar.setFixedWidth(200)
        self.nav_layout = QVBoxLayout(self.sidebar)
        self.nav_layout.setContentsMargins(0, 20, 0, 20)
        self.nav_layout.setSpacing(5)

        # 2. 页面容器 (StackedWidget)
        self.stacked_widget = FadeStackedWidget()

        # 3. 页面列表 (顺序决定了导航顺序)
        self.pages = {
            HomePage.PAGE_NAME: HomePage(self),
            CrawlerPage.PAGE_NAME: CrawlerPage(self),
            BatchExportPage.PAGE_NAME: BatchExportPage(self),
            MergePage.PAGE_NAME: MergePage(self),
            WideTablePage.PAGE_NAME: WideTablePage(self),
            XlsxToCsvPage.PAGE_NAME: XlsxToCsvPage(self),
            SettingsPage.PAGE_NAME: SettingsPage(self)

        }

        self.page_names_to_index: Dict[str, int] = {}  # 新增：用于快速查找页面索引
        self.buttons: List[QPushButton] = []

        for name, page in self.pages.items():
            index = self.stacked_widget.addWidget(page)
            self.page_names_to_index[name] = index  # 记录名称到索引的映射

            # --- 关键修改：连接 HomePage 的信号 ---
            if name == HomePage.PAGE_NAME:
                page.navigate_to_page.connect(self.navigate_to_page_by_name)

            # 创建导航按钮 (保持不变)
            btn = QPushButton(name)
            btn.setObjectName("NavButton")
            btn.setProperty("page_index", index)
            btn.clicked.connect(lambda checked, i=index, b=btn: self.switch_page(i, b))

            self.nav_layout.addWidget(btn)
            self.buttons.append(btn)

        self.nav_layout.addStretch()  # 将按钮推到顶部

        main_layout.addWidget(self.sidebar)
        main_layout.addWidget(self.stacked_widget)

        self.setCentralWidget(central_widget)

        # 初始显示第一个页面
        self.switch_page(0, self.buttons[0])

    @Slot(int, QPushButton)
    def switch_page(self, index, button):
        """切换显示的页面 (通过索引)"""
        self.stacked_widget.setCurrentIndex(index)

        # 更新按钮的激活状态样式
        for btn in self.buttons:
            btn.setProperty("class", "NavButton")
            btn.style().polish(btn)  # 重新渲染样式

        button.setProperty("class", "NavButton active")
        button.style().polish(button)  # 重新渲染样式

    @Slot(str)
    def navigate_to_page_by_name(self, page_name: str):
        """
        通过页面名称进行导航。
        此槽函数连接到 HomePage 发出的 navigate_to_page 信号。
        """
        if page_name in self.page_names_to_index:
            index = self.page_names_to_index[page_name]

            # 找到对应的导航按钮
            target_button = next((btn for btn in self.buttons if btn.property("page_index") == index), None)

            if target_button:
                self.switch_page(index, target_button)
            else:
                print(f"Error: Could not find button for page index {index}.")
        else:
            print(f"Error: Page name '{page_name}' not found in map.")




# --- 4. 应用程序启动 ---

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