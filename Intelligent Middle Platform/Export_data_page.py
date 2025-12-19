import sys
from typing import Dict, List, Optional
from PySide6.QtWidgets import (
    QFrame,QGraphicsDropShadowEffect, QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QLineEdit, QGridLayout,QPushButton, QMessageBox,QSizePolicy,QDateEdit,
    QProgressBar
)
from PySide6.QtCore import QDate,QThread
from PySide6.QtWidgets import QScrollArea # 确保在您的导入列表中
from PySide6.QtCore import Qt, Signal, QObject, Slot
from PySide6.QtGui import QColor
import os


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

