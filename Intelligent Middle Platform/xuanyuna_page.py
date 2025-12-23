import os
import time
import json  # 引入json模块
from typing import Dict

from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QLineEdit, QPushButton, QMessageBox, QProgressBar,
    QGridLayout, QFrame, QTextEdit, QScrollArea, QDialog, QFormLayout, QDialogButtonBox
)
from PySide6.QtCore import Qt, QThread, Signal, Slot, QDate, QTimer


# --- 1. 任务配置弹窗 (支持新增和修改) ---
class TaskConfigDialog(QDialog):
    def __init__(self, parent=None, name="", url=""):
        super().__init__(parent)
        self.setWindowTitle("任务配置")
        self.setFixedWidth(500)
        self.setStyleSheet("background-color: white;")

        layout = QFormLayout(self)
        self.name_input = QLineEdit(name)
        self.name_input.setPlaceholderText("例如：业务宽表数据")

        self.url_input = QTextEdit()
        self.url_input.setPlainText(url)
        self.url_input.setPlaceholderText("粘贴 direct_sub_url 地址...")
        self.url_input.setFixedHeight(120)

        layout.addRow("任务名称:", self.name_input)
        layout.addRow("页面 URL:", self.url_input)

        self.button_box = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        self.button_box.accepted.connect(self.accept)
        self.button_box.rejected.connect(self.reject)
        layout.addRow("", self.button_box)

    def get_data(self):
        return self.name_input.text().strip(), self.url_input.toPlainText().strip()


# --- 2. 任务卡片容器 ---
class TaskInputCard(QFrame):
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

        btn_layout = QHBoxLayout()
        self.btn_edit = QPushButton("编辑")
        self.btn_edit.setFixedSize(45, 24)
        self.btn_edit.setStyleSheet("font-size: 11px; color: #6366F1; border: 1px solid #6366F1; border-radius: 4px;")
        self.btn_edit.clicked.connect(lambda: self.edit_requested.emit(self.task_key))

        self.btn_del = QPushButton("✕")
        self.btn_del.setFixedSize(24, 24)
        self.btn_del.setStyleSheet("border:none; color: #9CA3AF; font-size: 14px; font-weight: bold;")
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

    def update_info(self, name, url):
        self.lbl_name.setText(name)
        self.task_url = url


class TaskConfigDialog(QDialog):
    def __init__(self, parent=None, name="", url=""):
        super().__init__(parent)
        # 隐藏原生边框，设置背景透明
        self.setWindowFlags(Qt.FramelessWindowHint | Qt.Dialog | Qt.WindowStaysOnTopHint)
        self.setAttribute(Qt.WA_TranslucentBackground)
        self.setFixedWidth(450)

        # 外层阴影容器
        self.container = QFrame(self)
        self.container.setStyleSheet("""
            QFrame {
                background-color: white;
                border-radius: 16px;
                border: 1px solid #E5E7EB;
            }
        """)

        main_layout = QVBoxLayout(self)
        main_layout.addWidget(self.container)

        # 内容布局
        layout = QVBoxLayout(self.container)
        layout.setContentsMargins(25, 25, 25, 25)
        layout.setSpacing(15)

        # 1. 标题
        header_lbl = QLabel("任务配置")
        header_lbl.setStyleSheet("font-size: 18px; font-weight: bold; color: #111827; border: none;")
        layout.addWidget(header_lbl)

        # 2. 任务名称输入
        name_label = QLabel("任务名称")
        name_label.setStyleSheet("color: #6B7280; font-size: 12px; font-weight: bold; border: none;")
        layout.addWidget(name_label)

        self.name_input = QLineEdit(name)
        self.name_input.setPlaceholderText("请输入任务名称...")
        self.name_input.setStyleSheet("""
            QLineEdit {
                padding: 10px;
                border: 1px solid #D1D5DB;
                border-radius: 8px;
                background-color: #F9FAFB;
                font-size: 14px;
            }
            QLineEdit:focus { border: 2px solid #6366F1; background-color: white; }
        """)
        layout.addWidget(self.name_input)

        # 3. 页面 URL 输入
        url_label = QLabel("页面 URL")
        url_label.setStyleSheet("color: #6B7280; font-size: 12px; font-weight: bold; border: none;")
        layout.addWidget(url_label)

        self.url_input = QTextEdit()
        self.url_input.setPlainText(url)
        self.url_input.setPlaceholderText("粘贴 target_url 地址...")
        self.url_input.setFixedHeight(120)
        self.url_input.setStyleSheet("""
            QTextEdit {
                padding: 10px;
                border: 1px solid #D1D5DB;
                border-radius: 8px;
                background-color: #F9FAFB;
                font-size: 13px;
            }
            QTextEdit:focus { border: 2px solid #6366F1; background-color: white; }
        """)
        layout.addWidget(self.url_input)

        layout.addSpacing(10)

        # 4. 按钮区
        btn_layout = QHBoxLayout()
        btn_layout.setSpacing(12)

        self.btn_cancel = QPushButton("取消")
        self.btn_cancel.setCursor(Qt.PointingHandCursor)
        self.btn_cancel.setFixedHeight(40)
        self.btn_cancel.setStyleSheet("""
            QPushButton {
                background-color: #F3F4F6;
                color: #374151;
                border-radius: 8px;
                font-weight: bold;
                border: none;
            }
            QPushButton:hover { background-color: #E5E7EB; }
        """)
        self.btn_cancel.clicked.connect(self.reject)

        self.btn_confirm = QPushButton("保存配置")
        self.btn_confirm.setCursor(Qt.PointingHandCursor)
        self.btn_confirm.setFixedHeight(40)
        self.btn_confirm.setStyleSheet("""
            QPushButton {
                background-color: #6366F1;
                color: white;
                border-radius: 8px;
                font-weight: bold;
                border: none;
            }
            QPushButton:hover { background-color: #4F46E5; }
        """)
        self.btn_confirm.clicked.connect(self.accept)

        btn_layout.addWidget(self.btn_cancel)
        btn_layout.addWidget(self.btn_confirm)
        layout.addLayout(btn_layout)
        # 记录一下布局对象，方便后面动态添加成功提示
        self.content_layout = layout

    # 支持鼠标拖动（因为去掉了标题栏）
    def mousePressEvent(self, event):
        if event.button() == Qt.LeftButton:
            self.m_dragPosition = event.globalPos() - self.frameGeometry().topLeft()
            event.accept()

    def mouseMoveEvent(self, event):
        if event.buttons() == Qt.LeftButton:
            self.move(event.globalPos() - self.m_dragPosition)
            event.accept()

    # --- 核心修改：重写 accept 方法 ---
    def accept(self):
        """当用户点击‘保存配置’按钮时，不再直接关闭，而是展示成功动画"""

        # 1. 检查数据是否完整（防止保存空配置）
        name = self.name_input.text().strip()
        url = self.url_input.toPlainText().strip()

        if not name or not url:
            # 如果没填，可以抖动一下或者变红，这里简单做个提示
            self.name_input.setStyleSheet(self.name_input.styleSheet() + "border: 1px solid #EF4444;")
            return

        self.container.setStyleSheet("background-color: white; border-radius: 16px; border: none;")

        # 2. 隐藏当前的输入控件和按钮，腾出空间
        self.name_input.hide()
        self.url_input.hide()
        self.btn_cancel.hide()
        self.btn_confirm.hide()

        # 3. 清理掉 layout 中原本存在的所有控件（标签、输入框等）
        # 这样可以防止残留的占位符导致布局错乱
        for i in reversed(range(self.container.layout().count())):
            item = self.container.layout().itemAt(i)
            if item.widget():
                item.widget().hide()

        # 3. 插入你喜欢的 HTML 风格成功文案
        success_lbl = QLabel(
            "<div style='text-align:center;'>"
            "<span style='font-size:60px;'>🎉</span><br><br>"
            "<b style='color:#27AE60; font-size:20px;'>配置保存成功！</b><br><br>"
            "<span style='color:#9CA3AF; font-size:14px;'>任务已同步至数据中心工作台</span>"
            "</div>"
        )
        success_lbl.setTextFormat(Qt.RichText)
        success_lbl.setAlignment(Qt.AlignCenter)
        self.content_layout.addWidget(success_lbl)

        # 4. 重点：使用 QTimer 延迟 1.2 秒后再真正执行父类的 accept()
        # 这时对话框才会关闭，并返回结果给主界面
        QTimer.singleShot(1200, lambda: super(TaskConfigDialog, self).accept())

    def get_data(self):
        return self.name_input.text().strip(), self.url_input.toPlainText().strip()


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
        self.btn_cancel.setStyleSheet(
            "background-color: #F3F4F6; color: #374151; border-radius: 8px; font-weight: bold; border: none;")
        self.btn_cancel.clicked.connect(self.reject)

        self.btn_confirm = QPushButton("确认删除")
        self.btn_confirm.setFixedHeight(38)
        self.btn_confirm.setStyleSheet(
            "background-color: #EF4444; color: white; border-radius: 8px; font-weight: bold; border: none;")
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

        top_bar = QHBoxLayout()
        # 推荐的标题样式
        title_main = QLabel("雷达 4.0")
        title_main.setStyleSheet("font-size: 26px; font-weight: 900; color: #1A1A1A; font-family: 'Impact';")
        title_sub = QLabel("ARK INFORMATION PROCESSING CENTRAL & DATA TERMINAL")
        title_sub.setStyleSheet("font-size: 9px; color: #AAA; font-weight: bold; letter-spacing: 2px;")
        self.btn_add = QPushButton("+ 新增任务")
        self.btn_add.clicked.connect(self.show_add_dialog)
        self.btn_add.setStyleSheet("""
            QPushButton {
                background: #6366F1;   /* 初始紫色 */
                color: white; 
                padding: 8px 18px; 
                border-radius: 6px; 
                font-weight: bold;
                border: none;
            }
            /* 悬停时：颜色稍微加深（更有确定感） */
            QPushButton:hover {
                background: #4F46E5; 
            }
            /* 按下时：颜色最深，并加入一个深色边框模拟凹陷 */
            QPushButton:pressed {
                background: #3730A3;
                border: 2px solid #312E81;
            }
        """)
        top_bar.addWidget(title_main)
        top_bar.addStretch()
        top_bar.addWidget(title_sub)
        top_bar.addStretch()
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
                    # 批量创建卡片
                    self.add_card(data['name'], key, data['url'], auto_save=False)
        except Exception as e:
            print(f"读取配置失败: {e}")

    def save_config(self):
        """将当前所有卡片保存到 JSON"""
        config_data = {}
        for key, card in self.task_cards.items():
            config_data[key] = {
                "name": card.lbl_name.text(),
                "url": card.task_url
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
            name, url = dialog.get_data()
            if name and url:
                key = f"T{int(time.time() * 1000)}"
                self.add_card(name, key, url)

    def show_edit_dialog(self, key):
        card = self.task_cards[key]
        dialog = TaskConfigDialog(self, name=card.lbl_name.text(), url=card.task_url)
        if dialog.exec():
            name, url = dialog.get_data()
            if name and url:
                card.update_info(name, url)
                self.save_config()  # 修改后保存

    def add_card(self, name, key, url, auto_save=True):
        yesterday = QDate.currentDate().addDays(-1)
        card = TaskInputCard(name, key, url, yesterday, yesterday)

        card.delete_requested.connect(self.remove_card)
        card.edit_requested.connect(self.show_edit_dialog)

        self.task_cards[key] = card
        self._relayout_cards()

        if auto_save:
            self.save_config()  # 新增后保存

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