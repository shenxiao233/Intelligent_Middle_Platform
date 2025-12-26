import os
import time
import json  # 引入json模块
from typing import Dict

from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,QComboBox,
    QLabel, QLineEdit, QPushButton, QMessageBox, QProgressBar,
    QGridLayout, QFrame, QTextEdit, QScrollArea, QDialog, QFormLayout, QDialogButtonBox
)
from PySide6.QtCore import Qt, QThread, Signal, Slot, QDate, QTimer,QObject
from SettingsPage import  SettingsPage

# 新增：自动化后台线程
class AutomationThread(QThread):
    finished_signal = Signal(str, bool, str, str)  # 参数：任务Key, 是否成功, 提示消息, 下载路径
    log_signal = Signal(str, str)  # 参数：任务Key, 日志内容

    def __init__(self, task_key, url, start_date, end_date, cookie_json):
        super().__init__()
        self.task_key = task_key
        self.url = url
        self.start_date = start_date
        self.end_date = end_date
        self.cookie_json = cookie_json

    def _log_callback(self, log_message):
        """日志回调函数，将日志通过信号发送到UI"""
        self.log_signal.emit(self.task_key, log_message)

    def run(self):
        try:
            # 这里调用我们之前写的 Worker 类
            from xuanyuan_worker import ElemeDataWorker  # 确保你能引用到之前的类

            worker = ElemeDataWorker(log_callback=self._log_callback)
            worker.inject_cookies(self.cookie_json)

            # 执行任务
            result_path = worker.run_task(self.url, self.start_date, self.end_date)

            worker.quit()

            if result_path:
                # 获取文件所在目录
                download_dir = os.path.dirname(result_path)
                self.finished_signal.emit(self.task_key, True, "同步完成", download_dir)
            else:
                self.finished_signal.emit(self.task_key, False, "同步失败：未找到文件", None)
        except Exception as e:
            self.finished_signal.emit(self.task_key, False, f"异常: {str(e)}", None)


class DownloadDispatcher(QObject):
    """
    这是一个全局管家类。
    它负责接收任务、管理排队，并确保同一时间只有一个 AutomationThread 在运行。
    """
    # 定义信号，用来告诉 UI 界面：任务状态变了
    task_added = Signal(dict)  # 任务进入队列了
    task_started = Signal(str)  # 某个任务正式开始下载了
    task_finished = Signal(str, bool, str, str)  # 某个任务跑完了 (key, success, msg, download_path)
    task_log_updated = Signal(str, str)  # 某个任务的日志更新了 (key, log_message)

    def __init__(self):
        super().__init__()
        self.queue = []  # 这是一个等待列表
        self.is_running = False  # 一个开关，记录当前是不是正忙
        self.current_worker = None

    def add_task(self, task_data):
        """
        外面点击“同步”按钮时，就调用这个方法把任务扔进来
        """
        self.queue.append(task_data)
        self.task_added.emit(task_data)  # 通知下载中心：来新活了，快显示出来
        self._check_next()

    def _check_next(self):
        """
        核心逻辑：检查是否可以跑下一个
        """
        # 如果当前没在忙，而且队列里还有人排队
        if not self.is_running and self.queue:
            task = self.queue.pop(0)  # 取出排在最前面的人
            self._execute_task(task)

    def _execute_task(self, task):
        self.is_running = True
        self.task_started.emit(task['key'])

        # 这里就是你原本的 AutomationThread
        self.worker = AutomationThread(
            task['key'], task['url'], task['start_date'],
            task['end_date'], task['cookie_json']
        )
        # 任务跑完后，通知 dispatcher
        self.worker.finished_signal.connect(self._on_finished)
        # 连接日志信号
        self.worker.log_signal.connect(self._on_log_updated)
        self.worker.start()
    
    def _on_log_updated(self, task_key, log_message):
        """处理任务日志更新"""
        self.task_log_updated.emit(task_key, log_message)

    def _on_finished(self, key, success, msg, download_path):
        self.is_running = False
        self.task_finished.emit(key, success, msg, download_path)
        # 重点：一个跑完了，立刻去找下一个
        self._check_next()


# --- 统一优化后的高级配置弹窗 ---
class TaskConfigDialog(QDialog):
    def __init__(self, parent=None, name="", url="", task_type="单页单表", config_info=None):
        super().__init__(parent)
        self.config_info = config_info if config_info else {}
        self.dynamic_inputs = {}  # 用于存储动态生成的输入框对象
        self.final_data = {}  # 用于存结果
        self.setWindowFlags(Qt.FramelessWindowHint | Qt.Dialog | Qt.WindowStaysOnTopHint)
        self.setAttribute(Qt.WA_TranslucentBackground)
        self.setFixedWidth(500)

        # 1. 创建外层容器
        self.container = QFrame(self)
        self.container.setStyleSheet("""
            QFrame { background-color: white; border-radius: 20px; border: 1px solid #E5E7EB; }
        """)

        # 2. 设置最外层布局 (包裹 container)
        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(10, 10, 10, 10)
        main_layout.addWidget(self.container)

        # 3. 创建内容布局 (核心改动：先创建，再设置对齐)
        self.content_layout = QVBoxLayout(self.container)
        self.content_layout.setContentsMargins(30, 30, 30, 30)
        self.content_layout.setSpacing(15)
        self.content_layout.setAlignment(Qt.AlignTop)  # ✅ 现在正确了，让控件从顶部开始排

        # --- 标题栏 ---
        header_layout = QHBoxLayout()
        header_lbl = QLabel("高级同步任务配置")
        header_lbl.setStyleSheet("font-size: 20px; font-weight: 800; color: #111827; border: none;")
        header_layout.addWidget(header_lbl)
        header_layout.addStretch()
        self.content_layout.addLayout(header_layout)

        # --- 统一输入控件样式函数 ---
        input_style = """
            QLineEdit, QTextEdit, QComboBox {
                padding: 10px 15px;
                border: 1.5px solid #E5E7EB;
                border-radius: 10px;
                background-color: #F9FAFB;
                font-size: 13px;
                color: #374151;
            }
            QLineEdit:focus, QTextEdit:focus, QComboBox:focus {
                border: 2px solid #6366F1;
                background-color: white;
            }
            QComboBox::drop-down {
                border: none;
                width: 30px;
            }
            QComboBox::down-arrow {
                image: none; /* 如果有图标可以替换 */
                border-left: 5px solid transparent;
                border-right: 5px solid transparent;
                border-top: 5px solid #6B7280;
                margin-right: 10px;
            }
            QComboBox QAbstractItemView {
                border: 1px solid #E5E7EB;
                border-radius: 8px;
                background-color: white;
                selection-background-color: #EEF2FF;
                selection-color: #6366F1;
                outline: none;
            }
        """

        # --- 表单输入区 ---
        self.add_label("任务名称")
        self.name_input = QLineEdit(name)
        self.name_input.setStyleSheet(input_style)
        self.content_layout.addWidget(self.name_input)

        self.add_label("页面 URL")
        self.url_input = QTextEdit(url)
        self.url_input.setFixedHeight(70)
        self.url_input.setStyleSheet(input_style)
        self.content_layout.addWidget(self.url_input)

        self.add_label("任务下载属性")
        self.type_combo = QComboBox()
        self.type_combo.addItems(["单页单表", "单页多表(无TAB)", "单页多表(有TAB)", "单页多表(有多级TAB)"])
        self.type_combo.setCurrentText(task_type)
        self.type_combo.setStyleSheet(input_style)
        self.type_combo.currentTextChanged.connect(self._on_type_changed)
        self.content_layout.addWidget(self.type_combo)

        # --- 动态 TAB 配置区域 ---
        self.tabs_area = QWidget()
        self.tabs_layout = QVBoxLayout(self.tabs_area)
        self.tabs_layout.setContentsMargins(0, 5, 0, 5)
        self.tabs_layout.setSpacing(10)
        self.content_layout.addWidget(self.tabs_area)
        self.tab_inputs = []

        # 4. 底部按钮
        btn_layout = QHBoxLayout()
        btn_layout.setSpacing(15)

        self.btn_cancel = QPushButton("取消")
        self.btn_cancel.clicked.connect(self.reject)
        self.btn_cancel.setCursor(Qt.PointingHandCursor)
        self.btn_cancel.setStyleSheet("""
                    QPushButton {
                        background-color: #F3F4F6; color: #4B5563; border-radius: 10px;
                        font-weight: bold; height: 42px; border: none; font-size: 14px;
                    }
                    QPushButton:hover { background-color: #E5E7EB; }
                """)

        self.btn_confirm = QPushButton("保存任务配置")
        self.btn_confirm.clicked.connect(self.accept)
        self.btn_confirm.setCursor(Qt.PointingHandCursor)
        self.btn_confirm.setStyleSheet("""
                    QPushButton {
                        background-color: #6366F1; color: white; border-radius: 10px;
                        font-weight: bold; height: 42px; border: none; font-size: 14px;
                    }
                    QPushButton:hover { background-color: #4F46E5; }
                """)

        btn_layout.addWidget(self.btn_cancel)
        btn_layout.addWidget(self.btn_confirm)

        # 将按钮布局放入主布局
        self.content_layout.addLayout(btn_layout)

        # --- 关键修改：在所有控件最后添加伸缩空间 ---
        # 这一行必须在 addLayout(btn_layout) 之后，确保它在最底部
        self.content_layout.addStretch()

        # --- 初始化显示：只保留一次调用 ---
        # 注意：此时 self.tab_inputs 应该已经初始化为 [] (在前面逻辑中)
        self._on_type_changed(self.type_combo.currentText())

    def add_label(self, text):
        lbl = QLabel(text)
        lbl.setStyleSheet("color: #6B7280; font-size: 12px; font-weight: 700; margin-top: 5px; border: none;")
        self.content_layout.addWidget(lbl)

    def _create_dynamic_input(self, key, label, placeholder):
        """通用动态输入框创建器"""
        lbl = QLabel(label)
        lbl.setStyleSheet("font-size: 12px; color: #4B5563; font-weight: 700; margin-top: 5px; border: none;")

        edit = QLineEdit()
        edit.setPlaceholderText(placeholder)

        # 回显逻辑
        if hasattr(self, 'config_info') and key in self.config_info:
            edit.setText(str(self.config_info[key]))

        edit.setStyleSheet("""
            QLineEdit {
                padding: 10px 15px; border: 1.5px solid #E5E7EB; border-radius: 10px;
                background-color: #F9FAFB; font-size: 13px; color: #374151;
            }
            QLineEdit:focus { border: 2px solid #6366F1; background-color: white; }
        """)

        self.tabs_layout.addWidget(lbl)
        self.tabs_layout.addWidget(edit)

        # 存入字典，以便 accept() 时通过 key 获取内容
        self.dynamic_inputs[key] = edit
        return edit


    def accept(self):
        """点击保存：先存数据，再清空 UI 展示动画"""

        # 1. 基础校验
        name_text = self.name_input.text().strip()
        url_text = self.url_input.toPlainText().strip()

        if not name_text or not url_text:
            self.name_input.setStyleSheet(self.name_input.styleSheet() + "border: 1.5px solid #EF4444;")
            return

        dynamic_data = {key: edit.text().strip() for key, edit in self.dynamic_inputs.items()}

        self.final_data = {
            "name": self.name_input.text().strip(),
            "url": self.url_input.toPlainText().strip(),
            "type": self.type_combo.currentText(),
            "config": dynamic_data  # 包含 table_names, level_1_tabs 等
        }

        # 2. 彻底清理布局展示动画
        self._clear_layout(self.content_layout)

        # 3. 动画界面美化
        self.container.setStyleSheet("background-color: white; border-radius: 20px; border: none;")
        self.content_layout.setAlignment(Qt.AlignCenter)
        self.content_layout.setContentsMargins(0, 0, 0, 0)

        success_lbl = QLabel(
            "<div style='text-align:center;'>"
            "<span style='font-size:60px;'>🎉</span><br><br>"
            "<b style='color:#10B981; font-size:22px;'>配置保存成功！</b><br><br>"
            "<span style='color:#9CA3AF; font-size:14px;'>任务已同步至数据中心工作台</span>"
            "</div>"
        )
        self.content_layout.addWidget(success_lbl)

        # 4. 延迟关闭
        QTimer.singleShot(1200, lambda: super(TaskConfigDialog, self).accept())

    def _clear_layout(self, layout):
        """递归清理布局辅助函数：这是防止元素堆叠的核心"""
        if layout is not None:
            while layout.count():
                item = layout.takeAt(0)
                widget = item.widget()
                if widget:
                    widget.deleteLater()  # 彻底销毁控件，释放空间
                elif item.layout():
                    self._clear_layout(item.layout())  # 递归清理子布局

    def get_data(self):
        """主界面调用此方法获取数据：改从存好的变量里拿，而不是从控件拿"""
        # 返回我们在 accept 里存好的 final_data
        return self.final_data

    # --- 修复拖拽报错 ---
    def mousePressEvent(self, event):
        if event.button() == Qt.LeftButton:
            # 初始化拖拽坐标
            self.m_dragPosition = event.globalPos() - self.frameGeometry().topLeft()
            event.accept()

    def mouseMoveEvent(self, event):
        # 增加 hasattr 判断，防止属性未初始化时报错
        if event.buttons() == Qt.LeftButton and hasattr(self, 'm_dragPosition'):
            self.move(event.globalPos() - self.m_dragPosition)
            event.accept()

    def _on_type_changed(self, text):
        """动态增减配置输入框逻辑"""
        self.setUpdatesEnabled(False)

        # 清理旧控件
        for i in reversed(range(self.tabs_layout.count())):
            widget = self.tabs_layout.itemAt(i).widget()
            if widget:
                widget.setParent(None)
                widget.deleteLater()

        self.dynamic_inputs = {}

        # --- 修复这里的调用，添加 key 参数 ---
        if "多表" in text:
            # 参数 1: key (用于保存数据), 参数 2: label, 参数 3: placeholder
            self._create_dynamic_input("table_names", "数据表名称", "例如：销售明细表, 库存汇总表")

        if "有TAB" in text:
            self._create_dynamic_input("level_1_tabs", "一级 TAB 名称列表", "例如：自营,零售,加盟")

        if "多级TAB" in text:
            self._create_dynamic_input("level_2_tabs", "多级 TAB 名称列表", "例如：日汇总,月汇总")

        self.setUpdatesEnabled(True)
        QTimer.singleShot(10, self.adjustSize)

    def adjust_window_size(self):
        """手动触发窗口尺寸自适应"""
        self.adjustSize()
        # 如果你希望保持动画感，这里可以加入 QPropertyAnimation

# --- 2. 任务卡片容器 ---
class TaskInputCard(QFrame):
    start_sync_requested = Signal(str)
    delete_requested = Signal(str)
    edit_requested = Signal(str)

    def __init__(self, name, key, url, start_date, end_date):
        super().__init__()
        self.task_key = key
        self.task_url = url
        self.setObjectName("TaskCard")
        self.setStyleSheet("""
            #TaskCard { background-color: #FFFFFF; border: 1px solid #E5E7EB; border-radius: 12px; }
            #TaskCard:hover { border-color: #6366F1; }
        """)

        layout = QVBoxLayout(self)

        header = QHBoxLayout()
        self.lbl_name = QLabel(name)
        self.lbl_name.setStyleSheet("font-size: 15px; font-weight: bold; color: #1F2937;")

        self.btn_edit = QPushButton("编辑")
        self.btn_edit.setFixedSize(45, 24)

        # 使用伪状态添加动态效果
        self.btn_edit.setStyleSheet("""
            QPushButton {
                font-size: 11px;
                color: #6366F1;
                border: 1px solid #6366F1;
                border-radius: 4px;
                background-color: transparent;
            }
            QPushButton:hover {
                background-color: #EEF2FF; /* 悬停时浅蓝色背景 */
                border-color: #4F46E5;
                color: #4F46E5;
            }
            QPushButton:pressed {
                background-color: #E0E7FF; /* 点击时加深背景 */
            }
        """)

        self.btn_edit.clicked.connect(lambda: self.edit_requested.emit(self.task_key))

        self.btn_del = QPushButton("✕")
        self.btn_del.setFixedSize(24, 24)
        self.btn_del.setStyleSheet("""
            QPushButton {
                border: none; color: #9CA3AF; font-size: 14px; font-weight: bold;
            }
            QPushButton:hover {
                color: #EF4444; 
            }
            QPushButton:pressed {
                padding-left: 2px;  /* 向右下角微移，产生点击感 */
                padding-top: 2px;
            }
        """)
        self.btn_del.clicked.connect(lambda: self.delete_requested.emit(self.task_key))

        header.addWidget(self.lbl_name)
        header.addStretch()
        header.addWidget(self.btn_edit)
        header.addWidget(self.btn_del)
        layout.addLayout(header)

        # 确保你导入了时间选择器
        try:
            from Export_data_page import CustomDateRangePicker
            self.date_picker = CustomDateRangePicker(start_date, end_date)
            layout.addWidget(self.date_picker)
        except:
            layout.addWidget(QLabel(f"时间范围: {start_date.toString(Qt.ISODate)}"))

        self.lbl_status = QLabel("● 就绪")
        self.lbl_status.setStyleSheet("color: #10B981; font-size: 12px; font-weight: bold; margin-bottom: 5px;")
        layout.addWidget(self.lbl_status)

        self.btn_export = QPushButton("开始同步数据")
        self.btn_export.setStyleSheet("""
            QPushButton { background-color: #EEF2FF; color: #4F46E5; border-radius: 6px; font-weight: bold; height: 35px; }
            QPushButton:hover { background-color: #6366F1; color: white; }
        """)
        layout.addWidget(self.btn_export)

        self.btn_export.clicked.connect(lambda: self.start_sync_requested.emit(self.task_key))


    def update_info(self, name, url):
        self.lbl_name.setText(name)
        self.task_url = url

    def set_loading(self, is_loading):
        """更新卡片状态 UI"""
        if is_loading:
            self.lbl_status.setText("● 正在同步中...")
            self.lbl_status.setStyleSheet("color: #6366F1; font-size: 12px; font-weight: bold;")
            self.btn_export.setEnabled(False)
            self.btn_export.setText("处理中...")
        else:
            self.lbl_status.setText("● 就绪")
            self.lbl_status.setStyleSheet("color: #10B981; font-size: 12px; font-weight: bold;")
            self.btn_export.setEnabled(True)
            self.btn_export.setText("开始同步数据")


# --- 3. 现代化删除确认弹窗 ---
class ModernConfirmDialog(QDialog):
    def __init__(self, parent=None, task_name=""):
        super().__init__(parent)
        self.setWindowFlags(Qt.FramelessWindowHint | Qt.Dialog | Qt.WindowStaysOnTopHint)
        self.setAttribute(Qt.WA_TranslucentBackground)
        self.setFixedSize(360, 200)

        self.container = QFrame(self)
        self.container.setGeometry(10, 10, 340, 180)
        self.container.setStyleSheet("background-color: white; border-radius: 12px; border: 1px solid #E5E7EB;")

        layout = QVBoxLayout(self.container)
        layout.setContentsMargins(25, 25, 25, 20)

        self.title_label = QLabel(f"确定删除任务「{task_name}」？")
        self.title_label.setWordWrap(True)
        self.title_label.setStyleSheet("font-size: 16px; font-weight: bold; color: #111827; border: none;")
        layout.addWidget(self.title_label)

        self.desc_label = QLabel("此操作无法撤销，请谨慎操作。")
        self.desc_label.setStyleSheet("font-size: 13px; color: #6B7280; border: none;")
        layout.addWidget(self.desc_label)

        layout.addStretch()

        btn_layout = QHBoxLayout()
        self.btn_cancel = QPushButton("取消")
        self.btn_cancel.setFixedHeight(38)
        # --- 取消按钮样式 ---
        self.btn_cancel.setStyleSheet("""
            QPushButton {
                background-color: #F3F4F6; 
                color: #374151; 
                border-radius: 8px; 
                font-weight: bold; 
                border: none;
            }
            QPushButton:hover {
                background-color: #E5E7EB; /* 悬停时稍微变深 */
            }
            QPushButton:pressed {
                background-color: #D1D5DB; /* 点击时明显变深 */
            }
        """)
        self.btn_cancel.clicked.connect(self.reject)

        self.btn_confirm = QPushButton("确认删除")
        self.btn_confirm.setFixedHeight(38)
        # --- 确认删除按钮样式 ---
        self.btn_confirm.setStyleSheet("""
            QPushButton {
                background-color: #EF4444; 
                color: white; 
                border-radius: 8px; 
                font-weight: bold; 
                border: none;
            }
            QPushButton:hover {
                background-color: #DC2626; /* 悬停时红色加深 */
            }
            QPushButton:pressed {
                background-color: #B91C1C; /* 点击时深红色 */
            }
        """)
        self.btn_confirm.clicked.connect(self.accept)

        btn_layout.addWidget(self.btn_cancel)
        btn_layout.addWidget(self.btn_confirm)
        layout.addLayout(btn_layout)

    def mousePressEvent(self, event):
        if event.button() == Qt.LeftButton:
            self.m_dragPosition = event.globalPos() - self.frameGeometry().topLeft()
            event.accept()

    def mouseMoveEvent(self, event):
        if event.buttons() == Qt.LeftButton:
            self.move(event.globalPos() - self.m_dragPosition)
            event.accept()


# --- 4. 轩辕数据主工作台 ---
class ExportWorkspacePage(QWidget):
    navigate_to_page = Signal(str)
    PAGE_NAME = "轩辕数据"

    def __init__(self, parent=None):
        super().__init__(parent)
        self.task_cards: Dict[str, TaskInputCard] = {}
        self.output_dir = os.path.join(os.getcwd(), 'mydata')
        self.config_path = os.path.join(os.getcwd(), 'task_config.json')  # 配置路径

        if not os.path.exists(self.output_dir): os.makedirs(self.output_dir)

        self._init_ui()
        self.load_config()  # 启动时读取配置

    def _init_ui(self):
        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(30, 30, 30, 30)

        # --- 修改 top_bar 部分 ---
        top_bar = QHBoxLayout()

        # 标题部分保持不变
        title_main = QLabel("雷达 4.0")
        title_main.setStyleSheet("font-size: 26px; font-weight: 900; color: #1A1A1A; font-family: 'Impact';")

        title_sub = QLabel("ARK INFORMATION PROCESSING CENTRAL & DATA TERMINAL")
        title_sub.setStyleSheet("font-size: 9px; color: #AAA; font-weight: bold; letter-spacing: 2px;")

        # 1. 新增：下载队列按钮 (采用描边风格)
        self.btn_queue = QPushButton("下载队列")
        self.btn_queue.setFixedSize(90, 32)  # 比新增按钮稍窄一点，保持错落感
        # 2. 绑定点击事件：发送信号并带上目标页面的名称 "下载中心"
        self.btn_queue.clicked.connect(lambda: self.navigate_to_page.emit("下载中心"))
        self.btn_queue.setStyleSheet("""
                QPushButton {
                    background: transparent;
                    color: #6366F1;
                    border: 1px solid #6366F1;
                    border-radius: 6px;
                    font-size: 12px;
                    font-weight: bold;
                }
                QPushButton:hover {
                    background: #F5F7FF;
                    border: 1px solid #4F46E5;
                }
                QPushButton:pressed {
                    background: #EEF2FF;
                }
            """)

        # 2. 原有的：新增任务按钮 (稍微统一一下尺寸)
        self.btn_add = QPushButton("+ 新增任务")
        self.btn_add.setFixedSize(100, 32)
        self.btn_add.clicked.connect(self.show_add_dialog)
        self.btn_add.setStyleSheet("""
                QPushButton {
                    background: #6366F1;
                    color: white; 
                    border-radius: 6px; 
                    font-weight: bold;
                    font-size: 12px;
                    border: none;
                }
                QPushButton:hover {
                    background: #4F46E5; 
                }
                QPushButton:pressed {
                    background: #3730A3;
                }
            """)

        # 3. 按照顺序添加到布局
        top_bar.addWidget(title_main)
        top_bar.addStretch()
        top_bar.addWidget(title_sub)
        top_bar.addStretch()

        # 将两个按钮并排添加
        top_bar.addWidget(self.btn_queue)
        top_bar.addSpacing(10)  # 两个按钮之间的间距
        top_bar.addWidget(self.btn_add)

        main_layout.addLayout(top_bar)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.NoFrame)
        self.scroll_content = QWidget()
        self.grid_layout = QGridLayout(self.scroll_content)
        self.grid_layout.setSpacing(20)
        self.grid_layout.setAlignment(Qt.AlignTop | Qt.AlignLeft)
        scroll.setWidget(self.scroll_content)
        main_layout.addWidget(scroll)

    # --- 持久化逻辑 ---
    def load_config(self):
        """从 JSON 读取配置"""
        if not os.path.exists(self.config_path):
            return
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                config_data = json.load(f)
                for key, data in config_data.items():
                    # 传入 JSON 中的 type 和 config
                    self.add_card(
                        data['name'], key, data['url'],
                        task_type=data.get('type', "单页单表"),
                        config_info=data.get('config', {}),
                        auto_save=False
                    )
        except Exception as e:
            print(f"读取配置失败: {e}")

    def save_config(self):
        """将当前所有卡片保存到 JSON"""
        config_data = {}
        for key, card in self.task_cards.items():
            config_data[key] = {
                "name": card.lbl_name.text(),
                "url": card.task_url,
                "type": getattr(card, 'task_type', "单页单表"),
                "config": getattr(card, 'config_info', {})  # 存储包含表名、TAB在内的完整字典
            }
        try:
            with open(self.config_path, 'w', encoding='utf-8') as f:
                json.dump(config_data, f, ensure_ascii=False, indent=4)
        except Exception as e:
            print(f"保存配置失败: {e}")

    # --- 交互逻辑 ---
    def show_add_dialog(self):
        dialog = TaskConfigDialog(self)
        if dialog.exec():
            # --- 核心修改：result 现在是一个字典 ---
            result = dialog.get_data()
            name = result.get("name")
            url = result.get("url")

            if name and url:
                key = f"T{int(time.time() * 1000)}"
                # 将整个 result 传给 add_card，或者分别传参
                self.add_card(name, key, url,
                              task_type=result.get("type"),
                              config_info=result.get("config"))

    def show_edit_dialog(self, key):
        card = self.task_cards[key]
        # 传入旧的 config 字典，以便弹窗回显表名和 TAB
        dialog = TaskConfigDialog(
            self,
            name=card.lbl_name.text(),
            url=card.task_url,
            task_type=getattr(card, 'task_type', "单页单表"),
            config_info=getattr(card, 'config_info', {})
        )

        if dialog.exec():
            result = dialog.get_data()

            # 更新卡片对象存储
            card.update_info(result["name"], result["url"])
            card.task_type = result["type"]
            card.config_info = result["config"]  # 这里包含了新的表名、TAB等所有动态内容

            self.save_config()  # 保存到本地 JSON

    def add_card(self, name, key, url, task_type="单页单表", config_info=None, auto_save=True):
        yesterday = QDate.currentDate().addDays(-1)
        card = TaskInputCard(name, key, url, yesterday, yesterday)

        # --- 新增：将高级配置存入卡片对象中 ---
        card.task_type = task_type
        card.config_info = config_info if config_info else {}

        # 绑定信号
        card.start_sync_requested.connect(self.handle_sync_start)
        card.delete_requested.connect(self.remove_card)
        card.edit_requested.connect(self.show_edit_dialog)

        self.task_cards[key] = card
        self._relayout_cards()

        if auto_save:
            self.save_config()

    def handle_sync_start(self, key):
        """
        根据 SITE_CONFIGS["轩辕"] 结构，精准提取并同步任务
        """
        # 1. 找到对应的卡片对象
        card = self.task_cards.get(key)
        if not card:
            return

        # 2. 采集 UI 上的日期范围
        start_dt = card.date_picker.start_date.toString("yyyy-MM-dd")
        end_dt = card.date_picker.end_date.toString("yyyy-MM-dd")

        # 3. 核心：根据站点配置提取 Cookie
        # 这里的 SettingsPage.get_all_cookies("轩辕") 内部会根据
        # SITE_CONFIGS["轩辕"] 的 Key 列表去获取对应的 QSettings 值
        raw_cookies_dict = SettingsPage.get_all_cookies("轩辕")

        cookie_list = []
        # 遍历返回的字典（它现在只包含 AEOLUS_MOZI_TOKEN, family, XY_TOKEN）
        if isinstance(raw_cookies_dict, dict):
            for name, value in raw_cookies_dict.items():
                if value and str(value).strip():  # 确保值不为空且非纯空格
                    cookie_list.append({
                        "domain": ".ele.me",
                        "name": str(name),
                        "value": str(value),
                        "path": "/"
                    })

        # 打印日志以便在控制台确认获取到的 Token 数量是否符合预期
        print(f"🚀 [任务准备] 卡片: {card.lbl_name.text()} | 站点: 轩辕 | 有效Cookie: {len(cookie_list)}个")


        # 4. 封装任务包
        task_data = {
            "key": key,
            "name": card.lbl_name.text(),
            "url": card.task_url,
            "start_date": start_dt,
            "end_date": end_dt,
            "cookie_json": json.dumps(cookie_list)
        }

        # 5. 提交给全局调度管家
        if hasattr(self, 'dispatcher') and self.dispatcher is not None:
            # 向管家队列添加任务
            self.dispatcher.add_task(task_data)

            # 更新当前轩辕页面卡片的 UI 状态
            # 先设为“琥珀色”代表正在排队，待管家真正启动时会变为“蓝色”
            card.set_loading(True)
            card.lbl_status.setText("● 已加入队列，排队中...")
            card.lbl_status.setStyleSheet("color: #F59E0B; font-size: 12px; font-weight: bold;")
        else:
            from PySide6.QtWidgets import QMessageBox
            QMessageBox.critical(self, "调度错误", "Dispatcher 未正确注入，请检查 MainWindow 的初始化顺序！")

    def connect_dispatcher_signals(self):
        """让轩辕页面听从管家的指挥"""
        if hasattr(self, 'dispatcher'):
            # 监听任务开始：更新对应卡片的 UI 状态
            self.dispatcher.task_started.connect(self._on_task_actually_started)
            # 监听任务结束：恢复对应卡片的 UI 状态（你已有的 handle_sync_finished）
            self.dispatcher.task_finished.connect(self.handle_sync_finished)

    def _on_task_actually_started(self, key):
        """当管家真正从队列里取出这个任务并开始跑时触发"""
        if key in self.task_cards:
            card = self.task_cards[key]
            card.set_loading(True)
            card.lbl_status.setText("● 正在同步中...")
            card.lbl_status.setStyleSheet("color: #6366F1; font-size: 12px; font-weight: bold;")

    @Slot(str, bool, str)
    def handle_sync_finished(self, key, success, message):
        card = self.task_cards[key]
        card.set_loading(False)

        if success:
            # 弹窗或修改状态
            card.lbl_status.setText("● 同步成功")
            card.lbl_status.setStyleSheet("color: #10B981; font-size: 12px; font-weight: bold;")
            # 可以通过对话框提示用户
            # QMessageBox.information(self, "完成", f"任务【{card.lbl_name.text()}】已下载完成")
        else:
            card.lbl_status.setText(f"● 失败: {message}")
            card.lbl_status.setStyleSheet("color: #EF4444; font-size: 12px; font-weight: bold;")


    def remove_card(self, key):
        card = self.task_cards.get(key)
        if not card: return

        dialog = ModernConfirmDialog(self, task_name=card.lbl_name.text())
        if dialog.exec() == QDialog.Accepted:
            card = self.task_cards.pop(key)
            self.grid_layout.removeWidget(card)
            card.deleteLater()

            self.save_config()  # 删除后保存
            QTimer.singleShot(50, self._relayout_cards)

    def _relayout_cards(self):
        for i in reversed(range(self.grid_layout.count())):
            item = self.grid_layout.itemAt(i)
            if item.widget():
                item.widget().setParent(None)

        for index, card in enumerate(self.task_cards.values()):
            self.grid_layout.addWidget(card, index // 2, index % 2)