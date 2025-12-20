import sys
from typing import Dict, List, Optional
from PySide6.QtWidgets import (
    QFrame,QGraphicsDropShadowEffect, QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QLineEdit, QGridLayout,QPushButton, QMessageBox,QSizePolicy,QDateEdit,
    QProgressBar,QAbstractSpinBox
)
from PySide6.QtCore import QDate,QThread
from PySide6.QtWidgets import QScrollArea # 确保在您的导入列表中
from PySide6.QtCore import Qt, Signal, QObject, Slot
from PySide6.QtGui import QColor
import os
from PySide6.QtWidgets import QMenu, QWidgetAction, QCalendarWidget, QGridLayout


class CustomDateRangePicker(QPushButton):
    """自定义双月日期范围选择按钮"""

    def __init__(self, start_date, end_date, parent=None):
        super().__init__(parent)
        self.start_date = start_date
        self.end_date = end_date
        self.setObjectName("DateRangePickerTrigger")
        self.update_text()
        self.setCursor(Qt.CursorShape.PointingHandCursor)
        self.clicked.connect(self.show_double_calendar)

    def update_text(self):
        # 将中间的箭头改为减号/分隔符，并调整为空格分隔
        # 格式：2025-12-19 - 2025-12-19
        self.setText(f"{self.start_date.toString('yyyy-MM-dd')}  -  {self.end_date.toString('yyyy-MM-dd')}")

    def show_double_calendar(self):
        menu = QMenu(self)
        menu.setObjectName("CalendarMenu")
        container = QWidget()
        container.setStyleSheet("background: white; border-radius: 8px;")
        layout = QHBoxLayout(container)
        layout.setContentsMargins(10, 10, 10, 10)
        layout.setSpacing(20)

        cal_left = QCalendarWidget()
        cal_right = QCalendarWidget()

        # 配置日历样式：隐藏周数，保持整洁
        for cal in [cal_left, cal_right]:
            cal.setVerticalHeaderFormat(QCalendarWidget.VerticalHeaderFormat.NoVerticalHeader)
            cal.setGridVisible(False)
            layout.addWidget(cal)

        # 初始日期
        cal_left.setSelectedDate(self.start_date)
        cal_right.setSelectedDate(self.end_date)

        # 选中逻辑
        cal_left.clicked.connect(lambda d: self._on_start_selected(d))
        cal_right.clicked.connect(lambda d: self._on_end_selected(d, menu))

        action = QWidgetAction(menu)
        action.setDefaultWidget(container)
        menu.addAction(action)
        menu.exec(self.mapToGlobal(self.rect().bottomLeft()))

    def _on_start_selected(self, date):
        self.start_date = date
        self.update_text()

    def _on_end_selected(self, date, menu):
        self.end_date = date
        self.update_text()
        menu.close()  # 选完结束日期自动关闭

# --- 预设默认 Team IDs ---
DEFAULT_TEAM_IDS: List[int] = [17440957, 17440962, 17440963, 17440964, 17440965, 17440967, 917535482]
# --- 导入真实 Worker ---
DEFAULT_TEAM_IDS_STR = ", ".join(map(str, DEFAULT_TEAM_IDS))


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
    def __init__(self, title: str, task_key: str, start_date_default: QDate, end_date_default: QDate, parent=None):
        super().__init__(parent)
        self.task_key = task_key
        self.setObjectName("TaskCard")
        self.setFixedWidth(360)
        self.setFixedHeight(160)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(20, 15, 20, 15)
        layout.setSpacing(12)

        # 1. 第一行：标题与状态
        header = QHBoxLayout()
        lbl_title = QLabel(title)
        lbl_title.setStyleSheet("font-size: 11pt; font-weight: bold; color: #303133; background: transparent;")
        self.lbl_status_single = QLabel("等待中")
        self.lbl_status_single.setStyleSheet("color: #909399; font-size: 9pt; background: transparent;")
        header.addWidget(lbl_title)
        header.addStretch()
        header.addWidget(self.lbl_status_single)
        layout.addLayout(header)

        # 2. 第二行：双月联动日期选择器
        self.date_picker = CustomDateRangePicker(start_date_default, end_date_default)
        layout.addWidget(self.date_picker)

        # 3. 第三行：操作按钮
        bottom_row = QHBoxLayout()
        self.btn_export_single = QPushButton("导出该任务")
        self.btn_export_single.setObjectName("SingleExportButton")
        self.btn_export_single.setFixedSize(90, 30)
        self.btn_export_single.setCursor(Qt.CursorShape.PointingHandCursor)

        bottom_row.addStretch()
        bottom_row.addWidget(self.btn_export_single)
        layout.addLayout(bottom_row)

    # --- 核心修复：补充缺失的方法 ---
    def get_dates(self) -> dict:
        return {
            "start": self.date_picker.start_date.toString("yyyy-MM-dd"),
            "end": self.date_picker.end_date.toString("yyyy-MM-dd")
        }

    def set_status(self, status: str, color: str):
        self.lbl_status_single.setStyleSheet(f"font-size: 9pt; color: {color}; background: transparent;")
        self.lbl_status_single.setText(status)

    def set_buttons_enabled(self, enabled: bool):
        """修复 AttributeError: 'TaskInputCard' object has no attribute 'set_buttons_enabled'"""
        self.btn_export_single.setEnabled(enabled)
        # 运行期间也禁用日期选择
        self.date_picker.setEnabled(enabled)


# --- 批量导出页面类 ---

class BatchExportPage(QWidget):
    PAGE_NAME = "批量数据导出"
    DESC = "支持多维度、大批量数据的一键云端同步下载"

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
        # 整体采用垂直布局
        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(0, 0, 0, 0)
        main_layout.setSpacing(0)

        # 1. 顶部标题和全局输入区
        header_widget = QWidget()
        header_widget.setObjectName("HeaderArea")
        header_layout = QVBoxLayout(header_widget)
        header_layout.setContentsMargins(50, 30, 50, 20)
        header_layout.setSpacing(15)

        # 标题行
        title_box = QHBoxLayout()

        title_text = QLabel("风神数据批量导出")
        title_text.setStyleSheet("font-size: 18pt; font-weight: bold; color: #1D1D1F; background: transparent;")
        title_box.addWidget(title_text)
        title_box.addStretch()
        header_layout.addLayout(title_box)

        # 团队 ID 输入行
        team_input_layout = QVBoxLayout()
        lbl_team_tip = QLabel("🛵 团队 ID 配置 (多个用逗号分隔)")
        lbl_team_tip.setStyleSheet("font-size: 10pt; color: #86868B; background: transparent;")
        self.entry_team_ids = QLineEdit()
        self.entry_team_ids.setText(DEFAULT_TEAM_IDS_STR)
        self.entry_team_ids.setFixedHeight(40)
        self.entry_team_ids.setPlaceholderText("请输入团队ID...")
        team_input_layout.addWidget(lbl_team_tip)
        team_input_layout.addWidget(self.entry_team_ids)
        header_layout.addLayout(team_input_layout)

        main_layout.addWidget(header_widget)

        # 2. 中间滚动卡片区 (无边框处理)
        self.scroll_area = QScrollArea()
        self.scroll_area.setWidgetResizable(True)
        self.scroll_area.setFrameShape(QFrame.Shape.NoFrame)
        self.scroll_area.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        self.scroll_area.setObjectName("PageScrollArea")
        self.scroll_area.setStyleSheet("background: transparent;")  # 增强透明感

        scroll_content = QWidget()
        scroll_content.setObjectName("ScrollContent")
        self.cards_grid = QGridLayout(scroll_content)
        self.cards_grid.setContentsMargins(50, 10, 50, 30)
        self.cards_grid.setSpacing(20)

        # 创建并添加卡片
        self.task_cards = self._create_task_cards()
        keys = list(self.task_cards.keys())
        for i, key in enumerate(keys):
            row, col = divmod(i, 2)
            self.cards_grid.addWidget(self.task_cards[key], row, col)

        self.cards_grid.setRowStretch(len(keys) // 2 + 1, 1)
        self.scroll_area.setWidget(scroll_content)
        main_layout.addWidget(self.scroll_area)

        # 3. 底部固定状态栏 (重构布局：移除停止按钮)
        self.bottom_bar = QFrame()
        self.bottom_bar.setObjectName("BottomBar")
        bottom_layout = QVBoxLayout(self.bottom_bar)
        bottom_layout.setContentsMargins(50, 20, 50, 30)  # 适当增加留白

        # 第一行：状态提示 + 打开目录 (保持在一条线上)
        status_line = QHBoxLayout()
        self.lbl_status = QLabel("准备就绪")
        self.lbl_status.setObjectName("StatusLabel")

        self.btn_open = QPushButton("📂 打开导出目录")
        self.btn_open.setObjectName("OpenFolderButton")  # 使用样式中定义的名称
        self.btn_open.setCursor(Qt.CursorShape.PointingHandCursor)
        self.btn_open.clicked.connect(self.open_output_directory)

        status_line.addWidget(self.lbl_status)
        status_line.addStretch()
        status_line.addWidget(self.btn_open)
        bottom_layout.addLayout(status_line)

        # 第二行：进度条 (置于按钮上方，更显专业)
        self.progress_bar = QProgressBar()
        self.progress_bar.setFixedHeight(4)
        self.progress_bar.setTextVisible(False)
        self.progress_bar.setValue(0)
        bottom_layout.addWidget(self.progress_bar)
        bottom_layout.addSpacing(15)

        # 第三行：全宽控制按钮
        # 移除 self.btn_stop 布局，直接添加全量按钮
        self.btn_start_batch = QPushButton("开始全量批量导出")
        self.btn_start_batch.setObjectName("StartButton")  # 对应 QSS 中的蓝色大按钮
        self.btn_start_batch.setFixedHeight(48)  # 增加高度，更具点击欲
        self.btn_start_batch.setCursor(Qt.CursorShape.PointingHandCursor)
        self.btn_start_batch.clicked.connect(self.start_batch_export)

        # 按钮设为 Expanding 以填满宽度
        self.btn_start_batch.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)

        bottom_layout.addWidget(self.btn_start_batch)

        main_layout.addWidget(self.bottom_bar)

    def apply_styles(self):
        """
        全量样式表定义，涵盖卡片、双月日期选择器、全局输入框及底部布局。
        修复了点击按钮发白的问题，并实现了 Web 端的极简视觉风格。
        """
        self.setStyleSheet("""
            /* --- 1. 全局基础样式 --- */
            QWidget { 
                background-color: #F8F9FB; 
                font-family: "PingFang SC", "Microsoft YaHei", "Segoe UI", sans-serif;
                color: #303133;
                outline: none; /* 移除点击时的虚线框 */
            }

            #ScrollContent, #HeaderArea { 
                background: transparent; 
            }

            /* --- 2. 顶部团队 ID 输入框 --- */
            QLineEdit {
                border: 1px solid #DCDFE6;
                border-radius: 8px;
                padding: 10px 15px;
                background-color: #FFFFFF;
                font-size: 11pt;
                selection-background-color: #409EFF;
            }
            QLineEdit:focus { 
                border: 1.5px solid #409EFF; 
            }

            /* --- 3. 任务卡片容器 --- */
            #TaskCard {
                background-color: #FFFFFF;
                border: 1px solid #EBEEF5;
                border-radius: 12px;
            }
            #TaskCard:hover { 
                border-color: #409EFF; 
            }

            /* --- 4. 联动日期选择按钮 (还原 image_ad591e) --- */
            #DateRangePickerTrigger {
                background-color: #FFFFFF;
                border: 1px solid #DCDFE6;
                border-radius: 4px;
                color: #606266;
                text-align: left;      /* 文字左对齐 */
                padding-left: 15px;    /* 留出左边距 */
                font-size: 10pt;
                height: 32px;
            }
            #DateRangePickerTrigger:hover { 
                border-color: #C0C4CC; 
                color: #409EFF;
            }
            #DateRangePickerTrigger:disabled { 
                background-color: #F5F7FA; 
                color: #C0C4CC; 
                border-color: #E4E7ED;
            }

            /* --- 5. 卡片内的小导出按钮 (修复点击变白问题) --- */
            #SingleExportButton {
                background-color: #F0F7FF;
                color: #409EFF;
                border: 1px solid #D9ECFF;
                border-radius: 4px;
                font-weight: 500;
                font-size: 9pt;
            }
            #SingleExportButton:hover { 
                background-color: #E1EFFF; 
                border-color: #B3D8FF;
            }
            #SingleExportButton:pressed { 
                background-color: #409EFF; /* 点击时变为明显的蓝色 */
                color: #FFFFFF;            /* 文字变白 */
                border-color: #409EFF;
            }
            #SingleExportButton:disabled { 
                background-color: #F5F7FA; 
                color: #C0C4CC; 
                border: none; 
            }

            /* --- 6. 底部固定状态栏 --- */
            #BottomBar {
                background-color: #FFFFFF;
                border-top: 1px solid #EBEEF5;
            }

            /* --- 7. 底部全量批量导出按钮 (image_ad6789) --- */
            #StartButton {
                background-color: #409EFF;
                color: white;
                border: none;
                border-radius: 8px;
                font-size: 12pt;
                font-weight: bold;
            }
            #StartButton:hover { 
                background-color: #66b1ff; 
            }
            #StartButton:pressed { 
                background-color: #3a8ee6; 
            }
            #StartButton:disabled { 
                background-color: #a0cfff; 
            }

            /* --- 8. 打开目录链接按钮 --- */
            #OpenFolderButton {
                background: transparent;
                color: #409EFF;
                border: none;
                font-size: 10pt;
                text-decoration: underline; /* 增加下划线模拟 Web 链接 */
            }
            #OpenFolderButton:hover { 
                color: #66b1ff; 
            }

            /* --- 9. 状态标签与进度条 --- */
            #StatusLabel { 
                color: #909399; 
                font-size: 10pt; 
            }
            #StatusLabelSmall { 
                color: #909399; 
                font-size: 9pt; 
                background: transparent; 
            }

            QProgressBar {
                background: #EBEEF5;
                border: none;
                border-radius: 2px;
            }
            QProgressBar::chunk {
                background-color: #409EFF;
                border-radius: 2px;
            }

            /* --- 10. 日历弹出面板美化 --- */
            #CalendarMenu {
                background-color: #FFFFFF;
                border: 1px solid #EBEEF5;
                border-radius: 8px;
            }
            QCalendarWidget QWidget { background-color: white; }
            QCalendarWidget QToolButton {
                color: #303133;
                background: transparent;
                border: none;
                font-weight: bold;
            }
            QCalendarWidget QAbstractItemView {
                selection-background-color: #409EFF;
                selection-color: white;
                outline: none;
            }
        """)

        # 为所有任务卡片添加弥散阴影逻辑
        for card in self.task_cards.values():
            shadow = QGraphicsDropShadowEffect(card)
            shadow.setBlurRadius(20)  # 阴影模糊程度
            shadow.setXOffset(0)  # 水平偏移
            shadow.setYOffset(4)  # 垂直偏移，产生悬浮感
            shadow.setColor(QColor(0, 0, 0, 15))  # 极浅的黑色阴影
            card.setGraphicsEffect(shadow)


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


    def _set_ui_running_state(self, is_running: bool, task_key: str = None):
        """设置整体和卡片的 UI 状态"""
        self.btn_start_batch.setEnabled(not is_running)

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

