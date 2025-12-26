from PySide6.QtWidgets import (
     QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QLineEdit,QPushButton, QMessageBox, QFileDialog, QSizePolicy,
)

from PySide6.QtCore import Qt, Signal
from PySide6.QtGui import QFont
import os
from worker import Worker




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