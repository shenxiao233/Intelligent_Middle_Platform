from typing import Dict
from PySide6.QtWidgets import (
    QApplication,QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QLineEdit, QPushButton, QMessageBox,QTabWidget
)
from PySide6.QtCore import QSettings
from PySide6.QtWidgets import QTextEdit # 新增导入
import json # 新增导入
from PySide6.QtWidgets import QScrollArea # 确保在您的导入列表中
from PySide6.QtCore import Qt


# --- 设置页面 (SettingsPage) ---
class SettingsPage(QWidget):
    PAGE_NAME = "设置"
    # 静态配置的 Cookie 字段名列表

    # --- 1. 配置：明确区分站点及其各自的 Cookie ---
    SITE_CONFIGS = {
        "风神": [
            "AEOLUS_MOZI_TOKEN",
            "xlly_s",
            "PASSPORT_TOKEN",
            "PASSPORT_AGENTS_TOKEN",
            "cna",
            "isg"
        ],
        "轩辕": [
            "AEOLUS_MOZI_TOKEN",  # 即使名字相同，在“轩辕”Tab下也是独立的
            "family",
            "XY_TOKEN"
        ]
    }


    SETTINGS_GROUP = "CrawlerSettings"

    def __init__(self, parent=None):
        super().__init__(parent)
        QApplication.setOrganizationName("YourCompanyName")
        QApplication.setApplicationName("AutomationToolbox")
        self.settings = QSettings()

        # 修改点：嵌套字典结构 { "站点名": { "Cookie名": QLineEdit } }
        self.entry_fields: Dict[str, Dict[str, QLineEdit]] = {}

        self._setup_ui()
        self.load_settings()

    def _setup_ui(self):
        main_page_layout = QVBoxLayout(self)
        main_page_layout.setContentsMargins(0, 0, 0, 0)

        scroll_area = QScrollArea()
        scroll_area.setWidgetResizable(True)
        scroll_area.setObjectName("SettingsScrollArea")
        scroll_area.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)

        content_widget = QWidget()
        content_widget.setObjectName("ContentCanvas")
        layout = QVBoxLayout(content_widget)
        layout.setContentsMargins(40, 40, 40, 40)
        layout.setSpacing(20)

        # 标题
        title = QLabel("⚙️ 应用程序设置")
        title.setObjectName("SettingsTitle")
        layout.addWidget(title)

        # 步骤 1: JSON 区域
        layout.addWidget(QLabel("步骤 1: 粘贴浏览器导出的 JSON 数据 (智能识别站点)"))
        self.entry_json = QTextEdit()
        self.entry_json.setPlaceholderText("粘贴 JSON 后点击解析，将自动填充到对应 Tab...")
        self.entry_json.setFixedHeight(100)
        layout.addWidget(self.entry_json)

        btn_parse = QPushButton("解析并分发 Cookie")
        btn_parse.setObjectName("ActionButton")
        btn_parse.clicked.connect(self.parse_json_cookies)
        layout.addWidget(btn_parse)

        layout.addWidget(self.create_separator())

        # 步骤 2: TAB 选项卡区域
        layout.addWidget(QLabel("步骤 2: 站点详细配置"))
        self.tab_widget = QTabWidget()
        self.tab_widget.setObjectName("SettingsTabs")

        for site_name, keys in self.SITE_CONFIGS.items():
            site_tab = QWidget()
            site_layout = QVBoxLayout(site_tab)
            site_layout.setSpacing(12)

            self.entry_fields[site_name] = {}

            for key in keys:
                hbox = QHBoxLayout()
                label = QLabel(key)
                label.setFixedWidth(150)
                label.setObjectName("CookieLabel")

                entry = QLineEdit()
                entry.setPlaceholderText(f"请输入 {key}")
                self.entry_fields[site_name][key] = entry

                hbox.addWidget(label)
                hbox.addWidget(entry, 1)
                site_layout.addLayout(hbox)

            site_layout.addStretch()
            self.tab_widget.addTab(site_tab, site_name)

        layout.addWidget(self.tab_widget)

        # 按钮
        btn_save = QPushButton("保存所有站点配置")
        btn_save.setObjectName("ActionButton")
        btn_save.setFixedHeight(45)
        btn_save.clicked.connect(self.save_settings)
        layout.addWidget(btn_save)

        self.lbl_status = QLabel("")
        self.lbl_status.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(self.lbl_status)

        layout.addStretch()
        scroll_area.setWidget(content_widget)
        main_page_layout.addWidget(scroll_area)

    def create_separator(self):
        line = QLabel()
        line.setFixedHeight(1)
        line.setStyleSheet("background-color: #D1D5DA; margin: 5px 0;")
        return line

    def parse_json_cookies(self):
        json_text = self.entry_json.toPlainText().strip()
        # 获取当前用户选中的 Tab 名字 (比如 "风神")
        current_site = self.tab_widget.tabText(self.tab_widget.currentIndex())

        try:
            cookie_array = json.loads(json_text)
            # 只取当前站点需要的字段
            fields = self.entry_fields[current_site]

            for cookie in cookie_array:
                name, value = cookie.get("name"), cookie.get("value")
                if name in fields:
                    fields[name].setText(value)

            self.lbl_status.setText(f"已更新【{current_site}】的配置")
        except:
            QMessageBox.warning(self, "错误", "解析失败")

    def load_settings(self):
        """按 站点/Key 的路径加载"""
        self.settings.beginGroup(self.SETTINGS_GROUP)
        for site_name, fields in self.entry_fields.items():
            for key, entry in fields.items():
                val = self.settings.value(f"{site_name}/{key}", "")
                entry.setText(str(val))
        self.settings.endGroup()

    def save_settings(self):
        """按 站点/Key 的路径保存"""
        self.settings.beginGroup(self.SETTINGS_GROUP)
        for site_name, fields in self.entry_fields.items():
            for key, entry in fields.items():
                self.settings.setValue(f"{site_name}/{key}", entry.text().strip())
        self.settings.endGroup()
        QMessageBox.information(self, "成功", "分站点配置已保存！")

    @staticmethod
    def get_all_cookies(site_name: str) -> Dict[str, str]:
        """
        只需传入 "风神" 或 "轩辕"
        """
        settings = QSettings()
        settings.beginGroup(SettingsPage.SETTINGS_GROUP)

        # 获取当前站点在 SITE_CONFIGS 中定义的字段名
        target_keys = SettingsPage.SITE_CONFIGS.get(site_name, [])

        site_cookies = {}
        for key in target_keys:
            # 核心：去对应的站点路径下取值
            value = settings.value(f"{site_name}/{key}", "")
            if value:
                site_cookies[key] = str(value)

        settings.endGroup()
        return site_cookies  # 返回这个站点的完整字典
