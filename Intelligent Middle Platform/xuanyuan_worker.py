import json
import time
import os
from datetime import datetime
from DrissionPage import ChromiumPage, ChromiumOptions


class ElemeDataWorker:
    def __init__(self, download_dir='mydata', log_callback=None):
        # 初始化配置
        self.co = ChromiumOptions()
        # 如果需要无头模式可以取消下面注释
        self.co.set_argument('--headless')
        self.page = ChromiumPage(self.co)

        # 建立下载目录
        self.target_path = os.path.join(os.getcwd(), download_dir)
        if not os.path.exists(self.target_path):
            os.makedirs(self.target_path)
        
        # 日志回调函数
        self.log_callback = log_callback

    def _get_now(self):
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    def _log(self, message):
        """记录日志"""
        if self.log_callback:
            self.log_callback(message)
        else:
            print(message)  # 兼容旧版本，没有回调时使用print

    def inject_cookies(self, cookie_json):
        """注入 Cookie"""
        cookies = json.loads(cookie_json)
        # 先访问域名建立上下文
        self.page.get("https://xy.ele.me")
        for cookie in cookies:
            try:
                self.page.set.cookies(cookie)
            except Exception as e:
                self._log(f"注入 Cookie 失败: {e}")

    def run_task(self, target_url, start_date, end_date):
        """执行主任务"""
        self._log(f"[{self._get_now()}] 启动任务: {target_url}")
        self.page.get(target_url)
        time.sleep(1)

        # 1. 切入 Iframe
        iframe = self.page.get_frame('.ark-lowcode-frame')
        if not iframe:
            self._log("❌ 未能找到业务框架，任务终止")
            return False

        # 2. 设置日期
        max_retries = 3
        for attempt in range(max_retries):
            try:
                start_input = iframe.ele('@placeholder=开始日期', timeout=10)
                end_input = iframe.ele('@placeholder=结束日期')

                if start_input and end_input:
                    # 填写开始日期
                    start_input.click()
                    iframe.run_js('arguments[0].removeAttribute("readonly");', start_input)
                    start_input.clear()
                    start_input.input(start_date)

                    # 填写结束日期
                    end_input.clear()
                    end_input.input(end_date)

                    # 回车确认触发页面刷新/监听
                    iframe.actions.key_down('ENTER').key_up('ENTER')

                    # --- 核心校验部分 ---
                    # 获取实际填入的值进行比对
                    actual_start = start_input.attr('value')
                    actual_end = end_input.attr('value')

                    if actual_start == start_date and actual_end == end_date:
                        self._log(f"[{self._get_now()}] ✅ 日期校验成功: {start_date} ~ {end_date}")
                        break  # 校验成功，跳出循环
                    else:
                        self._log(
                            f"[{self._get_now()}] ⚠️ 日期校验失败(实际为: {actual_start}), 正在进行第 {attempt + 1} 次重试...")

            except Exception as e:
                self._log(f"❌ 设置日期尝试中出错 (第{attempt + 1}次): {e}")

            # 如果是最后一次尝试仍然失败
            if attempt == max_retries - 1:
                self._log(f"❌ 经过 {max_retries} 次尝试，无法正确设置日期。")
                return False

        # 3. 点击查询并等待
        search_btn = (iframe.ele('text:查 询', timeout=2) or
                      iframe.ele('text:查询', timeout=2) or
                      iframe.ele('.ant-btn-primary.operation-btn', timeout=2))

        if search_btn:
            search_btn.click(by_js=True)
            loading_locator = 'text:正在努力为您查询'
            if iframe.ele(loading_locator, timeout=3):
                iframe.wait.ele_deleted(loading_locator, timeout=60)
            self._log(f"[{self._get_now()}] ✨ 数据加载完成")

        # 4. 执行下载
        return self._handle_download(start_date, end_date)

    def _handle_download(self, start_date, end_date):
        """内部下载处理逻辑"""
        self._log(f"[{self._get_now()}] 🔍 准备下载...")
        btn = self.page.ele('.ark-download-btn', timeout=15) or self.page.ele('text:下 载', timeout=5)

        if not btn:
            self._log("❌ 未找到下载按钮")
            return False

        check_minute = datetime.now().strftime("%Y%m%d%H%M")
        self.page.set.download_path(self.target_path)
        btn.click(by_js=True)

        # 轮询文件
        found_file = None
        for _ in range(60):  # 等待 180 秒
            files = os.listdir(self.target_path)
            for f in files:
                if check_minute in f and not f.endswith('.crdownload'):
                    found_file = f
                    break
            if found_file: break
            time.sleep(3)

        if found_file:
            return self._rename_file(found_file, start_date, end_date)
        return False

    def _rename_file(self, found_file, start_date, end_date):
        """文件重命名逻辑"""
        file_ext = os.path.splitext(found_file)[1]
        file_name_no_ext = os.path.splitext(found_file)[0]

        parts = file_name_no_ext.rsplit('_', 1)
        prefix = parts[0]

        new_name = f"{prefix}_{start_date}_至_{end_date}{file_ext}"
        old_path = os.path.join(self.target_path, found_file)
        new_path = os.path.join(self.target_path, new_name)

        if os.path.exists(new_path):
            os.remove(new_path)

        os.rename(old_path, new_path)
        self._log(f"[{self._get_now()}] ✅ 文件保存成功: {new_name}")
        return new_path

    def quit(self):
        self.page.quit()