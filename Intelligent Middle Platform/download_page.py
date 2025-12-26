import sys
import os
import json
import time
from datetime import datetime
from PySide6.QtWidgets import (
    QApplication,QFrame, QScrollArea,
    QTabWidget
)
from PySide6.QtCore import Signal
from PySide6.QtWidgets import QWidget, QVBoxLayout, QHBoxLayout, QLabel, QPushButton, QProgressBar, QTextEdit
from PySide6.QtCore import Qt, QRectF, Slot, QPropertyAnimation, QEasingCurve, QPoint
from PySide6.QtGui import QPainter, QColor, QPainterPath, QPen


class TaskItem(QWidget):
    def __init__(self, name, status_text, progress_val, duration, log_text, is_done=False, download_path=None):
        super().__init__()
        self.is_expanded = False
        self.header_height = 80
        self.log_height = 150
        self.is_done = is_done
        self.download_path = download_path
        self.setFixedHeight(self.header_height)

        # 必须设置，否则自绘可能不刷新
        self.setAttribute(Qt.WA_StyledBackground, True)

        # 1. 布局管理
        self.main_layout = QVBoxLayout(self)
        self.main_layout.setContentsMargins(0, 0, 0, 0)
        self.main_layout.setSpacing(0)

        # --- 顶部信息层 (确保背景透明，否则会遮挡自绘的圆角) ---
        self.header = QWidget()
        self.header.setFixedHeight(self.header_height)
        self.header.setAttribute(Qt.WA_TranslucentBackground)

        header_layout = QHBoxLayout(self.header)
        header_layout.setContentsMargins(20, 0, 20, 0)

        # 文字信息
        v_text = QVBoxLayout()
        name_lbl = QLabel(name)
        name_lbl.setStyleSheet("font-weight: bold; color: #111827; font-size: 14px; border:none;")
        time_lbl = QLabel(f"运行时间：{duration}")
        time_lbl.setStyleSheet("color: #94A3B8; font-size: 11px; border:none;")
        v_text.addStretch();
        v_text.addWidget(name_lbl);
        v_text.addWidget(time_lbl);
        v_text.addStretch()

        # 进度条
        v_prog = QVBoxLayout()
        speed_lbl = QLabel(status_text)
        status_color = "#10B981" if is_done else "#6366F1"
        speed_lbl.setStyleSheet(f"color: {status_color}; font-weight: bold; font-size: 11px;")
        bar = QProgressBar()
        bar.setValue(progress_val);
        bar.setFixedHeight(4);
        bar.setTextVisible(False)
        bar.setStyleSheet(f"QProgressBar {{ background: #F3F4F6; border:none; border-radius:2px; }} "
                          f"QProgressBar::chunk {{ background: {status_color}; border-radius:2px; }}")
        v_prog.addStretch();
        v_prog.addWidget(speed_lbl);
        v_prog.addWidget(bar);
        v_prog.addStretch()

        # 按钮区域
        btn_layout = QHBoxLayout()
        btn_layout.setSpacing(10)
        
        # 中止按钮 - 仅在未完成时显示
        if not self.is_done:
            btn_stop = QPushButton("中止")
            btn_stop.setFixedSize(56, 26)
            btn_stop.setStyleSheet(
                "QPushButton { color: #EF4444; border: 1px solid #EF4444; border-radius: 4px; font-weight:bold; background: white; }"
                "QPushButton:hover { background-color: #FEF2F2; }"
                "QPushButton:pressed { background-color: #FEE2E2; }"
            )
            btn_stop.setCursor(Qt.PointingHandCursor)
            btn_layout.addWidget(btn_stop)
        
        # 打开文件夹按钮 - 仅在完成时显示
        if self.is_done and self.download_path:
            btn_open = QPushButton("📂 打开文件夹")
            btn_open.setFixedSize(110, 26)
            btn_open.setStyleSheet(
                "QPushButton { color: #3B82F6; border: 1px solid #3B82F6; border-radius: 4px; font-weight:bold; background: white; }"
                "QPushButton:hover { background-color: #EFF6FF; border-color: #2563EB; }"
                "QPushButton:pressed { background-color: #DBEAFE; border-color: #1D4ED8; }"
            )
            btn_open.setCursor(Qt.PointingHandCursor)
            btn_open.clicked.connect(self.open_download_folder)
            btn_layout.addWidget(btn_open)

        header_layout.addLayout(v_text, 3)
        header_layout.addLayout(v_prog, 2)
        header_layout.addSpacing(20)
        header_layout.addLayout(btn_layout)

        # --- 日志层 ---
        self.log_box = QTextEdit()
        self.log_box.setReadOnly(True)
        self.log_box.setPlainText(log_text)
        self.log_box.setFixedHeight(self.log_height)
        # 必须透明，否则会盖住自绘的圆角边框
        self.log_box.setStyleSheet(
            "background: transparent; color: #34D399; border: none; padding: 15px; font-family: 'Consolas';")
        self.log_box.hide()

        self.main_layout.addWidget(self.header)
        self.main_layout.addWidget(self.log_box)

    def paintEvent(self, event):
        painter = QPainter(self)
        painter.setRenderHint(QPainter.Antialiasing)

        rect = self.rect()
        # 调整绘图区域，预留出1px边框宽度
        draw_rect = QRectF(rect.x() + 0.5, rect.y() + 0.5, rect.width() - 1, rect.height() - 1)
        radius = 8.0
        h = float(self.header_height)

        if not self.is_expanded:
            # 收起状态：单纯白色圆角矩形
            path = QPainterPath()
            path.addRoundedRect(draw_rect, radius, radius)
            painter.fillPath(path, QColor("white"))
        else:
            # 展开状态：黑白精准拼接
            # 1. 绘制上半部白色（上圆下尖）
            white_path = QPainterPath()
            white_path.setFillRule(Qt.WindingFill)
            white_path.addRoundedRect(draw_rect.x(), draw_rect.y(), draw_rect.width(), h, radius, radius)
            # 覆盖掉下方的圆角，使其变直
            white_path.addRect(draw_rect.x(), draw_rect.y() + h / 2, draw_rect.width(), h / 2)
            painter.fillPath(white_path, QColor("white"))

            # 2. 绘制下半部黑色（上尖下圆）
            black_path = QPainterPath()
            black_path.setFillRule(Qt.WindingFill)
            # 绘制整体下方区域
            black_path.addRoundedRect(draw_rect.x(), draw_rect.y() + h, draw_rect.width(), self.log_height - 1, radius,
                                      radius)
            # 覆盖掉上方的圆角，使其变直
            black_path.addRect(draw_rect.x(), draw_rect.y() + h, draw_rect.width(), radius)
            painter.fillPath(black_path, QColor("#0F172A"))

            # 3. 绘制中间分割线
            painter.setPen(QPen(QColor("#E5E7EB"), 1))
            painter.drawLine(draw_rect.x(), draw_rect.y() + h, draw_rect.right(), draw_rect.y() + h)

        # 4. 统一绘制外轮廓边框
        painter.setPen(QPen(QColor("#E5E7EB"), 1))
        border_path = QPainterPath()
        border_path.addRoundedRect(draw_rect, radius, radius)
        painter.drawPath(border_path)

    def mousePressEvent(self, event):
        # 将 event.pos() 替换为 event.position().toPoint()
        click_pos = event.position().toPoint()

        if self.childAt(click_pos) is not None and isinstance(self.childAt(click_pos), QPushButton):
            return

        if event.button() == Qt.LeftButton:
            self.toggle_log()

    def toggle_log(self):
        self.is_expanded = not self.is_expanded
        if self.is_expanded:
            self.log_box.show()
            self.setFixedHeight(self.header_height + self.log_height)
        else:
            self.log_box.hide()
            self.setFixedHeight(self.header_height)
        self.update()  # 触发重绘
    
    def open_download_folder(self):
        """打开下载文件夹"""
        if self.download_path and os.path.exists(self.download_path):
            try:
                if sys.platform.startswith('win'):
                    os.startfile(self.download_path)
                elif sys.platform.startswith('darwin'):
                    os.system(f'open "{self.download_path}"')
                else:
                    os.system(f'xdg-open "{self.download_path}"')
            except Exception as e:
                print(f"打开文件夹失败: {e}")


class DownloadCenterPage(QWidget):
    back_requested = Signal()

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("下载中心")
        self.resize(1100, 850)
        self.setStyleSheet("background-color: #FFFFFF;")

        # 1. 核心存储：记录 key 对应的 TaskItem 引用，方便后续更新进度或移除
        self.active_items = {}
        # 存储已完成任务的详细信息，用于持久化
        self.finished_tasks = []
        # 日志文件路径
        self.log_file_path = os.path.join(os.getcwd(), 'download_logs.json')

        self._init_ui()
        # 加载持久化的下载日志
        self.load_download_logs()

    def _init_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(30, 30, 30, 30)
        layout.setSpacing(0)
        layout.addSpacing(10)

        # 2. Tab Widget
        self.tabs = QTabWidget()
        self.tabs.setStyleSheet("""
            QTabWidget { border: none; }
            QTabWidget::pane { border: none; background: transparent; }
            QTabBar::tab {
                background: transparent; 
                min-width: 120px; 
                padding: 10px 10px;
                margin-right: 20px; 
                font-weight: bold; 
                font-size: 14px; 
                color: #94A3B8;
                border-bottom: 2px solid transparent;
            }
            QTabBar::tab:selected { 
                color: #6366F1; 
                border-bottom: 3px solid #6366F1; 
            }
        """)

        # 页签初始化
        self.dl_list_layout = self._create_list_tab("下载中 (0)")
        self.fi_list_layout = self._create_finished_tab("已完成")

        layout.addWidget(self.tabs)

        # --- 信号连接延迟处理 ---
        # 确保 dispatcher 被 MainWindow 注入后再连接
        from PySide6.QtCore import QTimer
        QTimer.singleShot(50, self._connect_signals)

    def _connect_signals(self):
        """连接管家信号"""
        if hasattr(self, 'dispatcher'):
            self.dispatcher.task_added.connect(self.add_new_task_item)
            self.dispatcher.task_started.connect(self.mark_task_as_running)
            self.dispatcher.task_finished.connect(self.on_task_finished)

    # --- 核心逻辑槽函数 ---

    @Slot(dict)
    def add_new_task_item(self, data):
        """[新增任务]：当点击同步按钮时，此函数触发"""
        key = data.get('key')
        name = data.get('name', '未知任务')

        # 创建一个初始状态的任务条
        item = TaskItem(
            name=name,
            status_text="排队等待中...",
            progress_val=0,
            duration="等待中",
            log_text="[QUEUE] 任务已加入单线程队列，等待调度..."
        )

        # 存储并添加到布局
        self.active_items[key] = item
        self.dl_list_layout.insertWidget(0, item)  # 永远放在最上面

        # 更新 Tab 标题数字
        self.tabs.setTabText(0, f"下载中 ({len(self.active_items)})")

    @Slot(str)
    def mark_task_as_running(self, key):
        """[开始下载]：当任务正式被调度运行"""
        if key in self.active_items:
            item = self.active_items[key]
            # 这里可以更新 UI 表现
            item.log_box.setPlainText("[INFO] 浏览器环境已就绪，正在抓取数据...")
            # 如果你有进度更新信号，也可以在这里继续扩展

    @Slot(str, bool, str, str)
    def on_task_finished(self, key, success, msg, download_path=None):
        """[任务结束]：移动到已完成"""
        if key in self.active_items:
            # 1. 从“下载中”移除
            old_item = self.active_items.pop(key)
            self.dl_list_layout.removeWidget(old_item)

            # 2. 在“已完成”页签创建一个新条目（或者复用旧的）
            status = "同步成功" if success else "同步失败"
            task_name = old_item.findChild(QLabel).text()
            finished_item = TaskItem(
                name=task_name,  # 获取之前的名字
                status_text=status,
                progress_val=100 if success else 0,
                duration="已完成",
                log_text=msg,
                is_done=success,
                download_path=download_path
            )
            self.fi_list_layout.insertWidget(0, finished_item)

            # 3. 保存任务到日志
            task_log = {
                'name': task_name,
                'status': status,
                'progress': 100 if success else 0,
                'duration': '已完成',
                'log_text': msg,
                'is_done': success,
                'download_path': download_path,
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            self.finished_tasks.append(task_log)
            self.save_download_logs()

            # 4. 销毁旧控件
            old_item.deleteLater()

            # 更新 Tab 标题数字
            self.tabs.setTabText(0, f"下载中 ({len(self.active_items)})")

    # --- UI 辅助方法 ---

    def _create_list_tab(self, name):
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.NoFrame)
        scroll.setStyleSheet("background: transparent;")
        content = QWidget()
        layout = QVBoxLayout(content)
        layout.setSpacing(15)
        layout.setContentsMargins(0, 20, 0, 20)
        layout.setAlignment(Qt.AlignTop)
        scroll.setWidget(content)
        self.tabs.addTab(scroll, name)
        return layout

    def _create_finished_tab(self, name):
        container = QWidget()
        v_layout = QVBoxLayout(container)
        v_layout.setContentsMargins(0, 10, 0, 0)
        tool_bar = QHBoxLayout()
        self.btn_clear = QPushButton(" 清空全部记录")
        self.btn_clear.setFixedSize(120, 32)
        self.btn_clear.setStyleSheet(
            "background: #EFF6FF; color: #3B82F6; border: none; border-radius: 6px; font-weight: bold; font-size: 12px;")
        self.btn_clear.clicked.connect(self.clear_download_records)
        tool_bar.addWidget(self.btn_clear)
        tool_bar.addStretch()
        v_layout.addLayout(tool_bar)
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.NoFrame)
        scroll.setStyleSheet("background: transparent;")
        content = QWidget()
        layout = QVBoxLayout(content)
        layout.setSpacing(15)
        layout.setContentsMargins(0, 15, 0, 20)
        layout.setAlignment(Qt.AlignTop)
        scroll.setWidget(content)
        v_layout.addWidget(scroll)
        self.tabs.addTab(container, name)
        return layout

    def save_download_logs(self):
        """保存下载日志到文件"""
        try:
            with open(self.log_file_path, 'w', encoding='utf-8') as f:
                json.dump(self.finished_tasks, f, ensure_ascii=False, indent=2)
            print(f"下载日志已保存到: {self.log_file_path}")
        except Exception as e:
            print(f"保存下载日志失败: {e}")

    def load_download_logs(self):
        """从文件加载下载日志"""
        if not os.path.exists(self.log_file_path):
            print("下载日志文件不存在，跳过加载")
            return
        
        try:
            with open(self.log_file_path, 'r', encoding='utf-8') as f:
                self.finished_tasks = json.load(f)
            
            # 重新创建已完成任务的UI
            for task_log in self.finished_tasks:
                finished_item = TaskItem(
                    name=task_log['name'],
                    status_text=task_log['status'],
                    progress_val=task_log['progress'],
                    duration=task_log['duration'],
                    log_text=task_log['log_text'],
                    is_done=task_log['is_done'],
                    download_path=task_log['download_path']
                )
                self.fi_list_layout.insertWidget(0, finished_item)
            
            print(f"成功加载 {len(self.finished_tasks)} 条下载记录")
        except Exception as e:
            print(f"加载下载日志失败: {e}")

    def clear_download_records(self):
        """清空下载记录"""
        if not self.finished_tasks:
            return
        
        # 实现清空动画
        self.animate_clear_records()

    def animate_clear_records(self):
        """清空记录的动画效果"""
        # 获取所有已完成任务的UI项
        items = []
        while self.fi_list_layout.count() > 0:
            item = self.fi_list_layout.itemAt(0)
            widget = item.widget()
            if widget:
                items.append(widget)
            self.fi_list_layout.removeItem(item)
        
        if not items:
            return
        
        # 批量动画
        def on_animation_finished():
            # 清理所有UI项
            for item in items:
                item.deleteLater()
            # 清空数据
            self.finished_tasks = []
            self.save_download_logs()
        
        # 创建动画
        animations = []
        for i, item in enumerate(items):
            # 透明度动画
            opacity_anim = QPropertyAnimation(item, b"windowOpacity")
            opacity_anim.setDuration(300)
            opacity_anim.setStartValue(1.0)
            opacity_anim.setEndValue(0.0)
            opacity_anim.setEasingCurve(QEasingCurve.InQuad)
            opacity_anim.setDelay(i * 50)  # 错开动画
            
            # 位置动画
            pos_anim = QPropertyAnimation(item, b"pos")
            pos_anim.setDuration(300)
            pos_anim.setStartValue(item.pos())
            pos_anim.setEndValue(item.pos() + QPoint(50, 0))  # 向右移动
            pos_anim.setEasingCurve(QEasingCurve.InQuad)
            pos_anim.setDelay(i * 50)
            
            animations.append(opacity_anim)
            animations.append(pos_anim)
        
        # 最后一个动画完成后清理数据
        if animations:
            animations[-1].finished.connect(on_animation_finished)
        else:
            on_animation_finished()
        
        # 启动所有动画
        for anim in animations:
            anim.start()