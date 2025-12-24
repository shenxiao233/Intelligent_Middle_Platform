import sys
import pandas as pd
import threading
import requests
from PySide6.QtWidgets import (
    QApplication, QMainWindow,QFrame,QGraphicsDropShadowEffect, QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QLineEdit, QGridLayout,QPushButton, QMessageBox, QFileDialog, QSizePolicy,
    QStackedWidget, QGraphicsOpacityEffect,QSystemTrayIcon,QMenu,
    QProgressBar
)
from PySide6.QtCore import QPropertyAnimation, QEasingCurve, QSize
from PySide6.QtCore import QTimer
from PySide6.QtWidgets import QTextEdit # 新增导入
from PySide6.QtCore import Qt, Signal, QObject, Slot,QThreadPool
from PySide6.QtGui import QFont, QColor,QFontMetrics
import os
from SettingsPage import  SettingsPage
from data_worker import DataWorker
from xlsx_to_csv_page import XlsxToCsvPage
from Export_data_page import BatchExportPage
from xuanyuna_page import ExportWorkspacePage
import qtawesome as qta
from download_page import DownloadCenterPage


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
        self.start_btn = QPushButton("启动数据处理与计算")
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
    PAGE_NAME = "风神离职数据"

    def __init__(self, parent=None):
        super().__init__(parent)
        self.worker: CrawlerWorker = None
        self._setup_ui()
        # --- 注意：彻底删除或注释掉 self.apply_styles() ---

    def _setup_ui(self):
        # 1. 顶层主布局：使用垂直布局并添加弹簧，使卡片在页面中心悬浮
        outer_layout = QVBoxLayout(self)
        outer_layout.setContentsMargins(0, 0, 0, 0)
        outer_layout.setSpacing(0)

        outer_layout.addStretch(1)  # 顶层弹簧

        # 2. 核心内容卡片 (Content Canvas)
        # 复用 style.qss 中的 #ContentCanvas 样式
        self.container_widget = QFrame()
        self.container_widget.setObjectName("ContentCanvas")
        self.container_widget.setFixedWidth(720)  # 保持舒适的宽度

        # 为卡片添加高级感阴影
        shadow = QGraphicsDropShadowEffect(self)
        shadow.setBlurRadius(35)
        shadow.setXOffset(0)
        shadow.setYOffset(10)
        shadow.setColor(QColor(0, 0, 0, 20))  # 极淡的黑色阴影
        self.container_widget.setGraphicsEffect(shadow)

        # 卡片内部布局
        container_layout = QVBoxLayout(self.container_widget)
        container_layout.setContentsMargins(50, 45, 50, 50)  # 内部大留白更有呼吸感
        container_layout.setSpacing(28)

        # 3. 标题区域 (居中显示)
        title_box = QHBoxLayout()

        title_text = QLabel("离职数据导出")
        title_text.setObjectName("SettingsTitle")  # 复用设置页大标题样式（深灰色）

        title_box.addStretch()
        title_box.addWidget(title_text)
        title_box.addStretch()
        container_layout.addLayout(title_box)

        # 4. 文件名输入区域
        input_group = QVBoxLayout()
        input_group.setSpacing(12)

        lbl_filename = QLabel("导出文件名")
        lbl_filename.setObjectName("StepLabel")  # 复用设置页步骤标签样式

        self.entry_filename = QLineEdit()
        self.entry_filename.setPlaceholderText("请输入导出文件名...")
        self.entry_filename.setFixedHeight(45)

        # --- 关键修复：重新生成并设置默认文件名 ---
        from datetime import datetime  # 确保已导入
        default_name = datetime.now().strftime("风神离职数据_%Y%m%d")
        self.entry_filename.setText(default_name)
        # ---------------------------------------

        input_group.addWidget(lbl_filename)
        input_group.addWidget(self.entry_filename)
        container_layout.addLayout(input_group)

        # 5. 操作按钮区域 (开始与停止)
        btn_layout = QHBoxLayout()
        btn_layout.setSpacing(15)

        # 开始按钮：复用紫色 ActionButton 样式
        self.btn_start = QPushButton("启动数据获取")
        self.btn_start.setObjectName("ActionButton")
        self.btn_start.setCursor(Qt.CursorShape.PointingHandCursor)
        self.btn_start.setFixedHeight(48)
        self.btn_start.clicked.connect(self.start_crawler)

        # 停止按钮：复用红色 StopButton 样式 (在 style.qss 中定义的)
        self.btn_stop = QPushButton("停止")
        self.btn_stop.setObjectName("StopButton")
        self.btn_stop.setCursor(Qt.CursorShape.PointingHandCursor)
        self.btn_stop.setFixedHeight(48)
        self.btn_stop.setFixedWidth(110)
        self.btn_stop.setEnabled(False)
        self.btn_stop.clicked.connect(self.stop_crawler)

        btn_layout.addWidget(self.btn_start, 1)  # 开始按钮占据主要宽度
        btn_layout.addWidget(self.btn_stop)
        container_layout.addLayout(btn_layout)

        # 6. 进度与状态反馈区域
        status_box = QVBoxLayout()
        status_box.setSpacing(12)

        self.lbl_status = QLabel("系统就绪，等待指令")
        self.lbl_status.setObjectName("StatusLabel")  # 复用状态文本样式
        self.lbl_status.setAlignment(Qt.AlignmentFlag.AlignCenter)

        self.progress_bar = QProgressBar()
        self.progress_bar.setValue(0)
        self.progress_bar.setTextVisible(False)  # 极简风格，不显示百分比文字
        self.progress_bar.setFixedHeight(8)

        status_box.addWidget(self.lbl_status)
        status_box.addWidget(self.progress_bar)
        container_layout.addLayout(status_box)

        # 7. 底部辅助操作 (打开文件夹)
        footer_layout = QHBoxLayout()
        self.btn_open = QPushButton("📂 查看导出目录")
        self.btn_open.setObjectName("SecondaryButton")  # 复用浅灰次要按钮样式
        self.btn_open.setCursor(Qt.CursorShape.PointingHandCursor)
        self.btn_open.setEnabled(False)
        self.btn_open.clicked.connect(self.open_output_directory)

        footer_layout.addStretch()
        footer_layout.addWidget(self.btn_open)
        footer_layout.addStretch()
        container_layout.addLayout(footer_layout)

        # 将配置好的卡片加入外层布局并居中
        outer_layout.addWidget(self.container_widget, alignment=Qt.AlignmentFlag.AlignCenter)

        outer_layout.addStretch(1)  # 底层弹簧



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
            self.lbl_status.setText(f" 任务完成！成功导出 {count} 条记录到：{os.path.basename(file_path)}")
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
    # 【核心 1】定义信号
    sig_status_update = Signal(str, str)

    def __init__(self):
        super().__init__()
        # 必须包含 WindowMinMaxButtonsHint 或 WindowSystemMenuHint
        # 这样 Windows 才会把该窗口列入 DWM 动画的管理名单
        self.setWindowFlags(
            Qt.FramelessWindowHint |
            Qt.Window |
            Qt.WindowMinimizeButtonHint  # 关键：允许任务栏交互
        )
        # 核心 2：允许透明，这是去掉最外层“方壳子”的关键
        self.setAttribute(Qt.WA_TranslucentBackground)

        self.setWindowTitle("Intelligent Middle Platform v2.0")
        self.resize(1300, 780)
        self.setMinimumSize(1220, 780)

        self.buttons = []
        self.page_names_to_index = {}

        # 页面配置
        self.page_config = [
            {"name": "首页", "icon": "fa5s.home", "class": HomePage},
            {"name": "风神离职数据", "icon": "fa5s.user-minus", "class": CrawlerPage},
            {"name": "批量数据导出", "icon": "fa5s.cloud-download-alt", "class": BatchExportPage},
            {"name": "轩辕数据", "icon": "fa5s.database", "class": ExportWorkspacePage},
            {"name": "下载中心", "icon": "fa5s.download", "class": DownloadCenterPage},
            {"name": "B端数据处理", "icon": "fa5s.project-diagram", "class": MergePage},
            {"name": "宽表导出", "icon": "fa5s.th-list", "class": WideTablePage},
            {"name": "CSV极速导出", "icon": "fa5s.file-excel", "class": XlsxToCsvPage},
            {"name": "设置", "icon": "fa5s.cog", "class": SettingsPage},
        ]

        # UI 初始化顺序：先设UI，再连信号，最后调样式
        self._setup_ui()
        self._setup_tray()
        self.apply_external_style()

        # 连接信号
        self.sig_status_update.connect(self.update_cookie_status)

        # 启动后延迟检测
        QTimer.singleShot(1000, self.check_cookie_realtime)

    def _setup_ui(self):
        # --- 核心：建立一个带有圆角的实体外壳 ---
        self.central_widget = QWidget()
        self.central_widget.setObjectName("CentralWidget")
        # 这里是修复限制线和圆角的关键
        self.central_widget.setStyleSheet("""
            #CentralWidget {
                background-color: #F8F9FA; 
                border-radius: 12px;
                border: 1px solid #E0E0E0; /* 可选：给个极细的淡灰色边框，比黑边好看 */
            }
        """)

        # 主布局，必须没有边距，否则会出现黑圈
        self.main_layout = QHBoxLayout(self.central_widget)
        self.main_layout.setContentsMargins(0, 0, 0, 0)
        self.main_layout.setSpacing(0)

        # --- 1. 侧边栏 ---
        self.sidebar = QWidget()
        self.sidebar.setObjectName("SideBar")
        self.sidebar.setFixedWidth(210)
        # 侧边栏左侧也需要设置圆角，否则会盖住外壳的圆角
        self.sidebar.setStyleSheet("""
            #SideBar { 
                background: transparent; 
                border-top-left-radius: 12px; 
                border-bottom-left-radius: 12px; 
            }
        """)
        self.nav_layout = QVBoxLayout(self.sidebar)
        self.nav_layout.setContentsMargins(15, 25, 15, 25)
        self.nav_layout.setSpacing(8)
        self._setup_breathing_light()

        # --- 2. 右侧内容区 (注意：只定义这一次 layout) ---
        self.content_outer_wrapper = QVBoxLayout()
        self.content_outer_wrapper.setContentsMargins(20, 0, 20, 20)  # 顶部改为0，由控制栏控制
        self.content_outer_wrapper.setSpacing(0)

        # --- 3. 标准控制栏 (使用 msc 矢量图标) ---
        self.window_controls = QHBoxLayout()
        self.window_controls.setContentsMargins(0, 0, 0, 0)
        self.window_controls.setSpacing(0)
        self.window_controls.addStretch()

        # 统一按钮样式
        ctrl_btn_style = """
                QPushButton {
                    background: transparent; border: none;
                    min-width: 45px; max-width: 45px; height: 32px;
                }
                QPushButton:hover { background-color: #E5E5E5; }
            """

        # 1. 最小化
        self.btn_min = QPushButton()
        # msc.chrome-minimize 是一条极细的横线
        self.btn_min.setIcon(qta.icon('msc.chrome-minimize', color='#666666'))
        self.btn_min.setIconSize(QSize(16, 16))
        self.btn_min.setStyleSheet(ctrl_btn_style)
        self.btn_min.clicked.connect(self.showMinimized)

        # 2. 最大化
        self.btn_max = QPushButton()
        # msc.chrome-maximize 是标准的细线正方形
        self.btn_max.setIcon(qta.icon('msc.chrome-maximize', color='#666666'))
        self.btn_max.setIconSize(QSize(16, 16))
        self.btn_max.setStyleSheet(ctrl_btn_style)
        self.btn_max.clicked.connect(self.toggle_maximized)

        # 3. 关闭
        self.btn_close = QPushButton()
        self.btn_close.setIcon(qta.icon('msc.chrome-close', color='#666666', color_active='white'))
        self.btn_close.setIconSize(QSize(16, 16))
        self.btn_close.setStyleSheet(ctrl_btn_style + """
                QPushButton:hover { 
                    background-color: #E81123; 
                    border-top-right-radius: 12px; 
                }
            """)
        self.btn_close.clicked.connect(self.close)

        self.window_controls.addWidget(self.btn_min)
        self.window_controls.addWidget(self.btn_max)
        self.window_controls.addWidget(self.btn_close)

        # --- 4. 页面主体白板容器 (ContentCanvas) ---
        self.content_container = QWidget()
        self.content_container.setObjectName("ContentCanvas")
        self.container_inner_layout = QVBoxLayout(self.content_container)
        self.container_inner_layout.setContentsMargins(0, 0, 0, 0)

        self.stacked_widget = QStackedWidget()
        self.container_inner_layout.addWidget(self.stacked_widget)

        # 阴影效果
        shadow = QGraphicsDropShadowEffect(self)
        shadow.setBlurRadius(35)
        shadow.setColor(QColor(0, 0, 0, 25))
        shadow.setOffset(0, 8)
        self.content_container.setGraphicsEffect(shadow)

        self.content_outer_wrapper.addWidget(self.content_container)

        # --- 5. 循环创建按钮逻辑 ---
        for i, conf in enumerate(self.page_config):
            page_inst = conf["class"](self)
            if hasattr(page_inst, 'navigate_to_page'):
                page_inst.navigate_to_page.connect(self.navigate_to_page_by_name)

            index = self.stacked_widget.addWidget(page_inst)
            self.page_names_to_index[conf["name"]] = index

            btn = QPushButton(f"  {conf['name']}")
            btn.setIcon(qta.icon(conf["icon"], color='#7F8C8D', color_active='#007AFF'))
            btn.setIconSize(QSize(18, 18))
            btn.setProperty("class", "NavButton")
            btn.setCheckable(True)
            btn.setAutoExclusive(True)
            btn.setCursor(Qt.PointingHandCursor)
            btn.clicked.connect(lambda checked, idx=index: self.switch_page(idx))

            self.nav_layout.addWidget(btn)
            self.buttons.append(btn)

        self.nav_layout.addStretch()

        # --- 6. 最终装配 (重新调整顺序) ---
        # 先把侧边栏加进去
        self.main_layout.addWidget(self.sidebar)

        # 创建一个新的垂直布局来包裹整个右侧区域
        self.right_main_container = QWidget()
        self.right_layout = QVBoxLayout(self.right_main_container)
        self.right_layout.setContentsMargins(0, 0, 0, 0)
        self.right_layout.setSpacing(0)

        # 1. 先把按钮栏放最上面
        # 注意：这里直接把 window_controls 作为一个独立的 Widget 包装，防止它被下方阴影覆盖
        self.controls_widget = QWidget()
        self.controls_widget.setLayout(self.window_controls)
        self.right_layout.addWidget(self.controls_widget)

        # 2. 再把原本的内容区内容加进去
        self.right_layout.addLayout(self.content_outer_wrapper)

        # 3. 最后把这个右侧大容器加入主布局
        self.main_layout.addWidget(self.right_main_container)

        self.setCentralWidget(self.central_widget)

    def toggle_maximized(self):
        """
        使用强制状态位切换，解决无边框窗口点击两次才能还原的问题
        """
        if self.isMaximized():
            # 关键：显式设置为 NoState (还原到正常窗口状态)
            self.setWindowState(Qt.WindowNoState)

            # 1. 恢复图标
            self.btn_max.setIcon(qta.icon('msc.chrome-maximize', color='#666666'))
            self.btn_max.setIconSize(QSize(16, 16))

            # 2. 恢复圆角和边框
            self.central_widget.setStyleSheet("""
                #CentralWidget { 
                    background-color: #F8F9FA; 
                    border-radius: 12px; 
                    border: 1px solid #E0E0E0; 
                }
            """)

            # 3. 恢复关闭按钮悬停圆角
            curr_style = self.btn_close.styleSheet()
            self.btn_close.setStyleSheet(
                curr_style.replace("border-top-right-radius: 0px;", "border-top-right-radius: 12px;"))

        else:
            # 关键：显式设置为 WindowMaximized
            self.setWindowState(Qt.WindowMaximized)

            # 1. 切换为还原图标
            self.btn_max.setIcon(qta.icon('msc.chrome-restore', color='#666666'))
            self.btn_max.setIconSize(QSize(16, 16))

            # 2. 全屏时去掉圆角
            self.central_widget.setStyleSheet("""
                #CentralWidget { 
                    background-color: #F8F9FA; 
                    border-radius: 0px; 
                    border: none; 
                }
            """)

            # 3. 强制关闭按钮为直角
            curr_style = self.btn_close.styleSheet()
            self.btn_close.setStyleSheet(
                curr_style.replace("border-top-right-radius: 12px;", "border-top-right-radius: 0px;"))

        # 暴力提升层级并强制刷新
        self.btn_min.raise_()
        self.btn_max.raise_()
        self.btn_close.raise_()
        self.central_widget.update()

    def _setup_breathing_light(self):
        """初始化呼吸灯，确保圆形不走样且水平对齐菜单"""
        self.status_header = QWidget()
        status_header_layout = QHBoxLayout(self.status_header)

        # 调整左边距：
        # 侧边栏整体有 15px 边距，这里再加 12px 左右，
        # 能让圆点中心与下方菜单图标中心完美垂直对齐。
        status_header_layout.setContentsMargins(18, 5, 0, 15)
        status_header_layout.setSpacing(12)

        # 呼吸灯圆点
        self.dot_light = QLabel()
        # 严格限制长宽相等，防止拉伸成方块
        self.dot_light.setFixedSize(8, 8)

        # 初始灰色状态，强制设置 border-radius 为高度的一半
        self.dot_light.setStyleSheet("""
            background-color: #BDC3C7; 
            border-radius: 4px; 
            border: none;
        """)

        # 状态文字
        self.status_text_label = QLabel("正在同步")
        self.status_text_label.setStyleSheet("""
            font-size: 13px; 
            font-weight: 600; 
            color: #7F8C8D;
            background: transparent;
        """)

        status_header_layout.addWidget(self.dot_light)
        status_header_layout.addWidget(self.status_text_label)
        status_header_layout.addStretch()

        self.nav_layout.addWidget(self.status_header)

        # 动画逻辑...
        self.opacity_effect = QGraphicsOpacityEffect(self.dot_light)
        self.dot_light.setGraphicsEffect(self.opacity_effect)
        self.breathing_anim = QPropertyAnimation(self.opacity_effect, b"opacity")
        self.breathing_anim.setDuration(1500)
        self.breathing_anim.setStartValue(0.3)
        self.breathing_anim.setEndValue(1.0)
        self.breathing_anim.setLoopCount(-1)
        self.breathing_anim.setEasingCurve(QEasingCurve.InOutQuad)
        self.breathing_anim.start()

    @Slot(str, str)
    def update_cookie_status(self, status: str, text: str):
        configs = {
            "valid": {"color": "#10B981", "label": "#2C3E50", "speed": 1800},
            "invalid": {"color": "#EF4444", "label": "#EF4444", "speed": 600},
            "loading": {"color": "#3B82F6", "label": "#7F8C8D", "speed": 1000},
            "error": {"color": "#F59E0B", "label": "#D97706", "speed": 1200}
        }
        cfg = configs.get(status, configs["error"])

        # 关键修复：每次更新背景色都要带上 border-radius
        self.dot_light.setStyleSheet(f"""
                background-color: {cfg['color']}; 
                border-radius: 4px; 
            """)

        self.status_text_label.setText(text)
        self.status_text_label.setStyleSheet(f"font-size: 13px; font-weight: 600; color: {cfg['label']};")

        # 动态调整速度
        self.breathing_anim.stop()
        self.breathing_anim.setDuration(cfg['speed'])
        self.breathing_anim.start()

    # --- 以下是原有逻辑方法，保持不变 ---
    def apply_external_style(self):
        style_path = os.path.join(os.path.dirname(__file__), "style.qss")
        if os.path.exists(style_path):
            with open(style_path, "r", encoding="utf-8") as f:
                self.setStyleSheet(f.read())

    def check_cookie_realtime(self):
        self.sig_status_update.emit("loading", "正在检测")

        def run_check():
            try:
                # cookies = SettingsPage.get_all_cookies()
                cookies = SettingsPage.get_all_cookies("风神")

                url = "https://httpizza.ele.me/lpd.meepo.mgmt/knight/queryKnightDimissionRecords"
                res = requests.get(url, params={'pageIndex': 1, 'pageSize': 20}, cookies=cookies, timeout=5)
                if res.status_code == 200 and str(res.json().get('code')) == '200':
                    print(res.json())
                    self.sig_status_update.emit("valid", "服务已连接")
                else:
                    self.sig_status_update.emit("invalid", "令牌已失效")
            except:
                self.sig_status_update.emit("error", "网络连接异常")

        threading.Thread(target=run_check, daemon=True).start()

    def mousePressEvent(self, event):
        if event.button() == Qt.LeftButton:
            pos = event.position()
            # 1. 判定是否点击在标题栏按钮上
            child = self.childAt(pos.toPoint())
            if child in [self.btn_min, self.btn_max, self.btn_close]:
                # 如果点到按钮，确保清除拖拽状态并退出
                if hasattr(self, 'm_dragPosition'): del self.m_dragPosition
                return

            # 2. 严格判定区域：仅限顶部 40px
            # 删除了 x < 210，确保点击侧边栏不会导致位移
            if pos.y() < 40:
                self.m_dragPosition = event.globalPos() - self.frameGeometry().topLeft()
                event.accept()
            else:
                # 3. 关键：点击其他区域时，必须删除该属性，否则 move 事件会误触发
                if hasattr(self, 'm_dragPosition'):
                    del self.m_dragPosition

    def mouseMoveEvent(self, event):
        # 只有存在 m_dragPosition 且当前不是最大化状态时才允许移动
        if event.buttons() == Qt.LeftButton and hasattr(self, 'm_dragPosition'):
            if self.isMaximized():
                return
            self.move(event.globalPos() - self.m_dragPosition)
            event.accept()

    def mouseReleaseEvent(self, event):
        # 4. 松开鼠标时彻底释放资源
        if hasattr(self, 'm_dragPosition'):
            del self.m_dragPosition

    @Slot(int)
    def switch_page(self, index):
        self.stacked_widget.setCurrentIndex(index)
        for i, btn in enumerate(self.buttons):
            btn.setProperty("active", i == index)
            btn.setChecked(i == index)
            btn.style().polish(btn)

    @Slot(str)
    def navigate_to_page_by_name(self, name):
        if name in self.page_names_to_index:
            self.switch_page(self.page_names_to_index[name])

    def _setup_tray(self):
        self.tray_icon = QSystemTrayIcon(self)
        self.tray_icon.setIcon(qta.icon('fa5s.toolbox', color='#2C3E50'))
        menu = QMenu()
        menu.addAction("还原窗口", self.showNormal)
        menu.addAction("彻底退出", QApplication.instance().quit)
        self.tray_icon.setContextMenu(menu)
        self.tray_icon.show()

# --- 应用程序启动 ---
if __name__ == "__main__":
    # 确保 QApplication 组织名和应用名设置在 QSettings 之前
    # QApplication.setOrganizationName("YourCompanyName")
    # # QApplication.setApplicationName("AutomationToolbox")

    # 1. 创建 QApplication 实例 (只创建一次)
    app = QApplication(sys.argv)

    # 2. 设置全局字体 (配置应用)
    # 建议使用更现代的字体或与您的 Apple 风格匹配的字体栈
    app.setFont(QFont("Microsoft YaHei UI", 10))

    # 3. 实例化并显示主窗口
    window = MainWindow()
    window.show()

    # 4. 运行应用主循环
    sys.exit(app.exec())