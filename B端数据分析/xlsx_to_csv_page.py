# 文件名: xlsx_to_csv_page.py
import polars as pl
import os
import platform
import subprocess
from concurrent.futures import ThreadPoolExecutor
from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel, QLineEdit,
    QPushButton, QFileDialog, QMessageBox, QComboBox, QProgressBar, QFrame, QDialog
)
from PySide6.QtCore import Qt, QThread, Signal, Slot


# --- 1. 自定义支持拖拽的输入框类 ---
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


# --- 2. 结果弹窗类 (现代圆角设计) ---
class CustomResultDialog(QDialog):
    def __init__(self, success_count, failure_count, output_path, parent=None):
        super().__init__(parent)
        self.output_path = output_path
        self.setWindowFlags(Qt.FramelessWindowHint | Qt.Dialog)
        self.setAttribute(Qt.WA_TranslucentBackground)
        self.setFixedSize(360, 300)
        self._init_ui(success_count, failure_count)

    def _init_ui(self, s, f):
        layout = QVBoxLayout(self)
        container = QFrame()
        container.setObjectName("DialogContainer")
        container.setStyleSheet("""
            #DialogContainer {
                background-color: white;
                border: 1px solid #E5E7EB;
                border-radius: 20px;
            }
            QLabel { background: transparent; border: none; }
        """)

        c_layout = QVBoxLayout(container)
        c_layout.setContentsMargins(30, 30, 30, 25)
        c_layout.setSpacing(15)

        icon_label = QLabel("🎉" if f == 0 else "ℹ️")
        icon_label.setAlignment(Qt.AlignCenter)
        icon_label.setStyleSheet("font-size: 48px;")

        title_label = QLabel("转换任务完成！" if f == 0 else "任务执行完毕")
        title_label.setAlignment(Qt.AlignCenter)
        title_label.setStyleSheet("font-size: 20px; font-weight: 800; color: #111827;")

        stats_box = QHBoxLayout()
        stats_box.addLayout(self._create_stat_item("成功", s, "#10B981"))
        stats_box.addLayout(self._create_stat_item("失败", f, "#EF4444" if f > 0 else "#9CA3AF"))

        btn_box = QHBoxLayout()
        btn_box.setSpacing(12)

        open_btn = QPushButton("📁 打开目录")
        open_btn.setCursor(Qt.PointingHandCursor)
        open_btn.setFixedSize(130, 40)
        open_btn.setStyleSheet("""
            QPushButton { background-color: #F3F4F6; color: #374151; border: none; border-radius: 10px; font-weight: bold; }
            QPushButton:hover { background-color: #E5E7EB; }
        """)
        open_btn.clicked.connect(self.open_folder)

        close_btn = QPushButton("我知道了")
        close_btn.setCursor(Qt.PointingHandCursor)
        close_btn.setFixedSize(130, 40)
        close_btn.setStyleSheet("""
            QPushButton { background-color: #6366F1; color: white; border: none; border-radius: 10px; font-weight: bold; }
            QPushButton:hover { background-color: #4F46E5; }
        """)
        close_btn.clicked.connect(self.accept)

        btn_box.addWidget(open_btn)
        btn_box.addWidget(close_btn)

        c_layout.addWidget(icon_label)
        c_layout.addWidget(title_label)
        c_layout.addLayout(stats_box)
        c_layout.addStretch()
        c_layout.addLayout(btn_box)
        layout.addWidget(container)

    def _create_stat_item(self, label, value, color):
        layout = QVBoxLayout()
        v_label = QLabel(str(value))
        v_label.setAlignment(Qt.AlignCenter)
        v_label.setStyleSheet(f"font-size: 28px; font-weight: bold; color: {color};")
        l_label = QLabel(label)
        l_label.setAlignment(Qt.AlignCenter)
        l_label.setStyleSheet("font-size: 13px; color: #6B7280;")
        layout.addWidget(v_label)
        layout.addWidget(l_label)
        return layout

    def open_folder(self):
        try:
            if platform.system() == "Windows":
                os.startfile(self.output_path)
            elif platform.system() == "Darwin":
                subprocess.run(["open", self.output_path])
            else:
                subprocess.run(["xdg-open", self.output_path])
        except Exception:
            pass
        self.accept()


# --- 3. 核心转换逻辑线程 ---
class ConversionThread(QThread):
    conversion_finished = Signal(int, int)
    progress_update = Signal(int, int)

    def __init__(self, input_path, output_path):
        super().__init__()
        self.input_path = input_path
        self.output_path = output_path

    def process_task(self, file_info):
        input_file, output_file = file_info
        try:
            df = pl.read_excel(input_file, engine="calamine", infer_schema_length=None)
            df.write_csv(output_file)
            return True
        except Exception as e:
            print(f"Error: {e}")
            return False

    def run(self):
        all_files = []
        if os.path.isfile(self.input_path):
            if self.input_path.lower().endswith('.xlsx'):
                all_files.append(self.input_path)
        else:
            all_files = [os.path.join(self.input_path, f) for f in os.listdir(self.input_path)
                         if f.lower().endswith('.xlsx') and not f.startswith('~$')]

        total = len(all_files)
        if total == 0:
            self.conversion_finished.emit(0, 0)
            return

        tasks = []
        for f in all_files:
            base_name = os.path.splitext(os.path.basename(f))[0]
            tasks.append((f, os.path.join(self.output_path, f"{base_name}.csv")))

        success_count, failure_count = 0, 0
        max_workers = min(os.cpu_count() or 4, total)

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            results = list(executor.map(self.process_task, tasks))
            for i, result in enumerate(results):
                if result:
                    success_count += 1
                else:
                    failure_count += 1
                self.progress_update.emit(i + 1, total)

        self.conversion_finished.emit(success_count, failure_count)


# --- 4. 页面 UI 类 (修复阴影补丁版) ---
class XlsxToCsvPage(QWidget):
    PAGE_NAME = "CSV极速导出"

    def __init__(self, parent=None):
        super().__init__(parent)
        self._init_ui()
        self._apply_style()

    def _init_ui(self):
        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(30, 30, 30, 30)
        main_layout.setSpacing(25)

        # 头部
        header = QVBoxLayout()
        title = QLabel("Excel 至 CSV 极速转换")
        title.setObjectName("HeaderLabel")
        desc = QLabel("基于 Polars 引擎，利用多核 CPU 并发实现大规模数据秒级导出。")
        desc.setObjectName("DescLabel")
        header.addWidget(title)
        header.addWidget(desc)
        main_layout.addLayout(header)

        # 配置卡片
        config_card = QFrame()
        config_card.setObjectName("ConfigCard")
        card_layout = QVBoxLayout(config_card)
        card_layout.setContentsMargins(25, 25, 25, 25)
        card_layout.setSpacing(20)

        # 模式 - 增加对齐修复
        mode_box = QVBoxLayout()
        mode_box.addWidget(QLabel("工作模式"), alignment=Qt.AlignLeft)
        self.mode_combo = QComboBox()
        self.mode_combo.addItems(["文件夹批量转换", "单个文件转换"])
        self.mode_combo.setFixedWidth(200)
        mode_box.addWidget(self.mode_combo)
        card_layout.addLayout(mode_box)

        # 输入 - 增加对齐修复
        in_box = QVBoxLayout()
        in_box.addWidget(QLabel("输入路径 (支持拖入 XLSX)"), alignment=Qt.AlignLeft)
        in_h = QHBoxLayout()
        self.input_edit = DropLineEdit()
        self.input_btn = QPushButton("📂 浏览")
        in_h.addWidget(self.input_edit)
        in_h.addWidget(self.input_btn)
        in_box.addLayout(in_h)
        card_layout.addLayout(in_box)

        # 输出 - 增加对齐修复
        out_box = QVBoxLayout()
        out_box.addWidget(QLabel("保存目录"), alignment=Qt.AlignLeft)
        out_h = QHBoxLayout()
        self.output_edit = DropLineEdit()
        self.output_btn = QPushButton("📁 选择")
        out_h.addWidget(self.output_edit)
        out_h.addWidget(self.output_btn)
        out_box.addLayout(out_h)
        card_layout.addLayout(out_box)

        main_layout.addWidget(config_card)

        # 底部动作
        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        self.convert_btn = QPushButton("🚀 开启并发转换")
        self.convert_btn.setObjectName("PrimaryBtn")
        self.convert_btn.setFixedHeight(50)

        main_layout.addWidget(self.progress_bar)
        main_layout.addWidget(self.convert_btn)
        main_layout.addStretch()

        self.input_btn.clicked.connect(self.select_input)
        self.output_btn.clicked.connect(self.select_output)
        self.convert_btn.clicked.connect(self.start_conversion)

    def _apply_style(self):
        self.setStyleSheet("""
            QWidget { background-color: #F3F4F6; font-family: 'Segoe UI', 'Microsoft YaHei'; }

            /* 核心修复：强制所有 Label 背景透明且无边框，彻底消除方块阴影 */
            QLabel { background-color: transparent; border: none; }

            #HeaderLabel { font-size: 24px; font-weight: 800; color: #111827; }
            #DescLabel { font-size: 13px; color: #6B7280; }

            #ConfigCard { background-color: white; border: 1px solid #E5E7EB; border-radius: 12px; }
            #ConfigCard QLabel { 
                font-size: 14px; 
                font-weight: 600; 
                color: #374151; 
                margin-bottom: 2px;
            }

            QLineEdit { border: 1px solid #D1D5DB; border-radius: 8px; padding: 10px; background: white; color: #111827; }
            QLineEdit:focus { border: 2px solid #6366F1; }

            QComboBox { border: 1px solid #D1D5DB; border-radius: 8px; padding: 5px 10px; background: white; }

            QPushButton { background-color: white; border: 1px solid #D1D5DB; border-radius: 8px; padding: 8px 15px; font-weight: bold; color: #374151; }
            QPushButton:hover { background-color: #F9FAFB; }

            #PrimaryBtn { background-color: #6366F1; color: white; border: none; font-size: 16px; border-radius: 10px; }
            #PrimaryBtn:hover { background-color: #4F46E5; }

            QProgressBar { border: none; background-color: #E5E7EB; height: 6px; border-radius: 3px; }
            QProgressBar::chunk { background-color: #6366F1; border-radius: 3px; }
        """)

    def select_input(self):
        if self.mode_combo.currentText() == "单个文件转换":
            p, _ = QFileDialog.getOpenFileName(self, "选择文件", "", "Excel Files (*.xlsx)")
        else:
            p = QFileDialog.getExistingDirectory(self, "选择文件夹")
        if p: self.input_edit.setText(os.path.normpath(p))

    def select_output(self):
        p = QFileDialog.getExistingDirectory(self, "选择保存目录")
        if p: self.output_edit.setText(os.path.normpath(p))

    def start_conversion(self):
        in_p, out_p = self.input_edit.text(), self.output_edit.text()
        if not in_p or not out_p:
            return QMessageBox.warning(self, "提示", "请完整填写路径。")

        self.convert_btn.setEnabled(False)
        self.convert_btn.setText("⚡ 极速处理中...")
        self.progress_bar.setVisible(True)
        self.progress_bar.setValue(0)

        self.thread = ConversionThread(in_p, out_p)
        self.thread.progress_update.connect(lambda c, t: self.progress_bar.setValue(int(c / t * 100)))
        self.thread.conversion_finished.connect(self.on_finished)
        self.thread.start()

    def on_finished(self, s, f):
        self.convert_btn.setEnabled(True)
        self.convert_btn.setText("🚀 开启并发转换")
        self.progress_bar.setVisible(False)
        dialog = CustomResultDialog(s, f, self.output_edit.text(), self)
        dialog.exec()