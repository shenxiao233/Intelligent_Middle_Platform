import json
import time
import os
import re
from datetime import datetime, timedelta
from DrissionPage import ChromiumPage, ChromiumOptions

# 尝试导入polars，如果不存在则提示安装
try:
    import polars as pl
    POLARS_AVAILABLE = True
except ImportError:
    POLARS_AVAILABLE = False


def parse_date(date_str):
    """解析日期字符串为datetime对象"""
    return datetime.strptime(date_str, '%Y-%m-%d')

def format_date(date_obj):
    """格式化datetime对象为日期字符串"""
    return date_obj.strftime('%Y-%m-%d')

def split_date_range(start_date, end_date, max_days=8):
    """将日期范围分割为多个不超过max_days天的批次"""
    start = parse_date(start_date)
    end = parse_date(end_date)
    batches = []
    
    current_start = start
    while current_start <= end:
        # 计算这批次的结束日期
        batch_end = min(current_start + timedelta(days=max_days-1), end)
        
        # 添加批次
        batches.append({
            'start': format_date(current_start),
            'end': format_date(batch_end)
        })
        
        # 移动到下一天
        current_start = batch_end + timedelta(days=1)
    
    return batches

def extract_timestamp_from_filename(filename):
    """从文件名中提取时间戳"""
    timestamp_match = re.search(r'(\d{14,17})', filename)
    if timestamp_match:
        timestamp_str = timestamp_match.group(1)
        try:
            if len(timestamp_str) >= 14:
                file_time = datetime.strptime(timestamp_str[:14], '%Y%m%d%H%M%S')
                if len(timestamp_str) > 14:
                    microsecond_part = timestamp_str[14:]
                    if len(microsecond_part) <= 6:
                        microsecond = int(microsecond_part.ljust(6, '0'))
                        file_time = file_time.replace(microsecond=microsecond)
                return file_time
        except ValueError:
            pass
    return None


class ElemeDataWorker:
    def __init__(self, download_dir='mydata', log_callback=None):
        # 建立下载目录
        self.target_path = os.path.join(os.getcwd(), download_dir)
        if not os.path.exists(self.target_path):
            os.makedirs(self.target_path)
        
        # 初始化配置
        self.co = ChromiumOptions()
        # 如果需要无头模式可以取消下面注释
        self.co.set_argument('--headless')  # 注释掉无头模式，便于调试
        # 设置下载目录
        self.co.set_argument('--download-default-directory', self.target_path)
        # 添加一些其他有用的参数
        self.co.set_argument('--disable-extensions')
        self.co.set_argument('--disable-dev-shm-usage')
        
        self.page = ChromiumPage(self.co)
        
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

    def run_task(self, target_url, start_date, end_date, task_type=None, config_info=None, task_name=None):
        """执行主任务"""
        self._log(f"[{self._get_now()}] 启动任务: {target_url}")
        self._log(f"[{self._get_now()}] 任务类型: {task_type}")
        self._log(f"[{self._get_now()}] 配置信息: {config_info}")
        if task_name:
            self._log(f"[{self._get_now()}] 任务名称: {task_name}")

        # 建立任务类型到处理方法的映射
        task_handlers = {
            "单页单表": self.run_single_page_task,
            "自定义看板": self.run_custom_dashboard_task,
            # 可以继续添加其他任务类型
            # "多页多表": self.run_multi_page_task,
            # "自定义导出": self.run_custom_export_task,
        }

        # 根据任务类型获取对应的处理方法
        handler = task_handlers.get(task_type)
        if handler:
            self._log(f"[{self._get_now()}] 🎯 使用任务类型: {task_type}")
            return handler(target_url, start_date, end_date, config_info, task_name)
        else:
            self._log(f"[{self._get_now()}] ❌ 未知的任务类型: {task_type}")
            self._log(f"[{self._get_now()}] 📋 可用的任务类型: {list(task_handlers.keys())}")
            return self.target_path

    def run_single_page_task(self, target_url, start_date, end_date, config_info=None, task_name=None):
        """执行单页单表任务（原逻辑）"""
        self.page.get(target_url)
        time.sleep(1)

        # 1. 切入 Iframe
        iframe = self.page.get_frame('.ark-lowcode-frame')
        if not iframe:
            self._log("❌ 未能找到业务框架，任务终止")
            return self.target_path

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
                return self.target_path

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
            return self.target_path

        check_minute = datetime.now().strftime("%Y%m%d%H%M")
        self.page.set.download_path(self.target_path)
        btn.click(by_js=True)

        # 轮询文件
        found_file = None
        for _ in range(300):  # 等待 300 秒
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
        return self.target_path

    def run_custom_dashboard_task(self, target_url, start_date, end_date, config_info=None, task_name=None):
        """执行自定义看板任务
        
        参数:
            task_name: 任务名称，用于生成文件名
            
        返回值:
            str: 合并文件或第一个下载文件的路径，如果失败则返回目标目录路径
        """
        self._log(f"[{self._get_now()}] 🚀 启动自定义看板任务")
        
        try:
            # 1. 访问目标URL
            self.page.get(target_url)
            time.sleep(1)
            
            # 2. 切入 Iframe (注意：自定义看板使用不同的iframe选择器)
            iframe = self.page.get_frame('.xy-shell__content-frame')
            if not iframe:
                self._log("❌ 未能找到业务框架，任务终止")
                return False
            
            # 3. 将日期范围分割为批次
            self._log(f"[{self._get_now()}] 📅 分割日期范围: {start_date} ~ {end_date}")
            batches = split_date_range(start_date, end_date, max_days=7)
            self._log(f"[{self._get_now()}] 📊 分割为 {len(batches)} 个批次")
            
            # 4. 处理每个批次
            batch_exports = []
            export_start_times = []
            
            for i, batch in enumerate(batches, 1):
                self._log(f"[{self._get_now()}] 🔄 开始处理批次 {i}/{len(batches)}: {batch['start']} ~ {batch['end']}")
                
                result = self._process_date_batch(iframe, batch, i, len(batches))
                if result['success']:
                    batch_exports.append(result)
                    export_start_times.append(result['export_start_time'])
                    self._log(f"[{self._get_now()}] ✅ 批次 {i} 处理成功")
                else:
                    self._log(f"[{self._get_now()}] ❌ 批次 {i} 处理失败")
            
            # 5. 所有批次处理完成后，跳转到下载记录页面进行批量下载
            if batch_exports:
                self._log(f"[{self._get_now()}] 🚀 跳转到下载记录页面...")
                self.page.get("https://xy.ele.me/xy-bbjs")
                time.sleep(3)
                
                # 6. 点击下载记录
                self._log(f"[{self._get_now()}] 🔍 打开下载记录列表...")
                time.sleep(1)
                record_btn = self.page.ele('text:下载记录') or self.page.ele('@data-spm-anchor-id')
                if record_btn:
                    record_btn.click()
                    time.sleep(3)
                else:
                    self._log(f"[{self._get_now()}] ⚠️ 未找到下载记录按钮")
                    self.page.refresh()
                    time.sleep(3)
                
                # 7. 批量下载逻辑
                # 8. 获取实际下载的行索引
                downloaded_rows = self._handle_batch_download(export_start_times, start_date, end_date)
                if downloaded_rows:
                    self._log(f"[{self._get_now()}] ✅ 批量下载完成，共下载 {len(downloaded_rows)} 个文件")
                else:
                    self._log(f"[{self._get_now()}] ⚠️ 批量下载未完全成功")
                
                # 9. 构建时间窗口用于验证
                # 使用实际的导出时间窗口（从第一个批次开始到最后一个批次结束后30秒）
                if export_start_times:
                    # 修复时间窗口计算问题：将开始时间提前2秒，确保所有相关文件都能被包含在内
                    actual_window_start = min(export_start_times) - timedelta(seconds=5)
                    actual_window_end = max(export_start_times) + timedelta(seconds=30)
                    time_windows = [(actual_window_start, actual_window_end)]
                    self._log(f"[{self._get_now()}] 📅 文件验证时间窗口: {actual_window_start.strftime('%Y-%m-%d %H:%M:%S')} ~ {actual_window_end.strftime('%Y-%m-%d %H:%M:%S')}")
                else:
                    time_windows = []
                
                # 10. 验证文件下载，使用实际下载的行索引而不是所有批次
                matched_files = self._verify_download_files_with_retry(
                    time_windows, downloaded_rows, max_retries=15, retry_interval=5
                )
                
                # 10. 合并文件 - 添加类型检查防止布尔值错误
                merged_file_path = None  # 默认情况下，没有合并文件路径
                
                if POLARS_AVAILABLE and matched_files and isinstance(matched_files, list) and len(matched_files) > 0:
                    self._log(f"[{self._get_now()}] 📊 开始合并文件...")
                    merged_file_path = self._merge_downloaded_files(self.target_path, start_date, end_date, task_name, matched_files)
                    if merged_file_path:
                        self._log(f"[{self._get_now()}] ✅ 文件合并完成，合并文件路径: {merged_file_path}")
                    else:
                        self._log(f"[{self._get_now()}] ⚠️ 文件合并未成功完成，但已下载单个文件")
                elif not POLARS_AVAILABLE:
                    self._log(f"[{self._get_now()}] ⚠️ Polars未安装，跳过文件合并功能")
                else:
                    self._log(f"[{self._get_now()}] ⚠️ 文件验证结果为空或类型不正确，跳过文件合并")
                    self._log(f"[{self._get_now()}] 🔍 matched_files类型: {type(matched_files)}, 值: {matched_files}")
                
                # 如果有匹配的文件，即使没有合并文件，也返回第一个文件的路径
                if matched_files and len(matched_files) > 0:
                    return self.target_path  # 返回保存目录，统一打开文件夹行为
                else:
                    # 如果没有任何匹配文件，返回目标目录路径
                    return self.target_path
            else:
                self._log(f"[{self._get_now()}] ❌ 没有成功处理的批次")
                return self.target_path
                
        except Exception as e:
            self._log(f"[{self._get_now()}] ❌ 自定义看板任务执行异常: {e}")
            return self.target_path

    def _process_date_batch(self, iframe, batch, batch_number, total_batches):
        """处理单个日期批次的数据下载"""
        self._log(f"[{self._get_now()}] 📅 批次 {batch_number}/{total_batches}: {batch['start']} ~ {batch['end']}")
        # 初始记录一个时间作为备份
        export_start_time = datetime.now()


        try:
            # 重新获取iframe句柄，确保是最新的
            iframe = self.page.get_frame('.xy-shell__content-frame')

            # 等待页面稳定
            time.sleep(2)

            # 1. 清空筛选条件 - 增加重试机制
            self._log(f"[{self._get_now()}] 🧹 清空筛选条件...")
            clear_success = False
            for retry in range(3):
                try:
                    clear_btns = iframe.eles('.filter-item-delete', timeout=3)
                    if len(clear_btns) >= 4:
                        clear_btns[3].click()
                        clear_success = True
                        self._log(f"[{self._get_now()}] ✅ 成功清空筛选条件")
                        break
                    else:
                        self._log(f"[{self._get_now()}] ⚠️ 清空按钮不足，尝试第{retry+1}次...")
                        self.page.refresh()  # 刷新当前页面
                        time.sleep(3)  # 刷新后给予充足的加载时间
                except Exception as e:
                    self._log(f"[{self._get_now()}] ⚠️ 清空操作失败，尝试第{retry+1}次: {e}")
                    self.page.refresh()
                    time.sleep(3)

                if not clear_success:
                    time.sleep(1)

            if not clear_success:
                self._log(f"[{self._get_now()}] ⚠️ 清空筛选条件失败，但继续执行...")

            time.sleep(1)

            # 2. 设置日期范围 - 增加严格校验循环
            self._log(f"[{self._get_now()}] 📅 设置日期范围并校验...")
            date_set_success = False

            for retry in range(4):  # 增加到4次尝试
                start_input = iframe.ele('@placeholder=开始日期', timeout=5)
                end_input = iframe.ele('@placeholder=结束日期', timeout=5)

                if start_input and end_input:
                    # 移除只读
                    iframe.run_js('arguments[0].removeAttribute("readonly");', start_input)
                    iframe.run_js('arguments[0].removeAttribute("readonly");', end_input)

                    # 清空并输入
                    start_input.clear().input(batch['start'])
                    time.sleep(0.2) # 给前端一点反应时间
                    end_input.clear().input(batch['end'])

                    # 关键：手动触发一次失去焦点或回车
                    iframe.actions.key_down('ENTER').key_up('ENTER')
                    time.sleep(0.5)

                    # --- 校验逻辑 ---
                    actual_start = start_input.attr('value')
                    actual_end = end_input.attr('value')

                    if actual_start == batch['start'] and actual_end == batch['end']:
                        self._log(f"[{self._get_now()}] ✅ 日期校验一致: {actual_start} ~ {actual_end}")
                        date_set_success = True
                        break
                    else:
                        self._log(f"[{self._get_now()}] ⚠️ 日期校验失败(实际:{actual_start}~{actual_end})，重试第{retry+1}次...")
                        # 如果失败，尝试先点击一下空白处或再次清除
                        iframe.click((0,0))
                        time.sleep(0.5)
                else:
                    self._log(f"[{self._get_now()}] ⚠️ 未找到输入框，重试中...")
                    time.sleep(1)

            if not date_set_success:
                self._log(f"[{self._get_now()}] ❌ 无法正确设置日期，跳过此批次")
                return {'success': False, 'export_start_time': export_start_time, 'batch_info': f"{batch['start']} ~ {batch['end']}"}
            
            # 3. 开始分析 - 增加重试机制
            self._log(f"[{self._get_now()}] 🔍 点击开始分析...")
            analysis_success = False
            for retry in range(3):
                try:
                    analyze_btn = iframe.ele('text:开始分析', timeout=5)
                    if analyze_btn:
                        analyze_btn.click()
                        analysis_success = True
                        self._log(f"[{self._get_now()}] ✅ 成功点击开始分析")
                        break
                    else:
                        self._log(f"[{self._get_now()}] ⚠️ 开始分析按钮未找到，尝试第{retry+1}次...")
                except Exception as e:
                    self._log(f"[{self._get_now()}] ⚠️ 点击开始分析失败，尝试第{retry+1}次: {e}")
                
                if not analysis_success:
                    time.sleep(2)
            
            if not analysis_success:
                self._log(f"[{self._get_now()}] ❌ 点击开始分析失败")
                return {
                    'success': False,
                    'export_start_time': export_start_time,
                    'batch_info': f"{batch['start']} ~ {batch['end']}"
                }
            
            self._log(f"[{self._get_now()}] 🔍 等待数据查询...")
            try:
                iframe.wait.ele_deleted('text:正在努力为您查询', timeout=60)
                self._log(f"[{self._get_now()}] ✅ 数据查询完成")
            except:
                self._log(f"[{self._get_now()}] ⚠️ 查询完成状态检查超时，但继续执行...")
            
            time.sleep(3)

            result = self._handle_custom_download(iframe, export_start_time, batch)
            return result
            
        except Exception as e:
            self._log(f"[{self._get_now()}] ❌ 批次处理过程中出现异常: {e}")
            return {
                'success': False,
                'export_start_time': export_start_time,
                'batch_info': f"{batch['start']} ~ {batch['end']}"
            }

    def _handle_custom_download(self, iframe, export_start_time, batch):
        """处理自定义下载逻辑（长按触发菜单）"""
        self._log(f"[{self._get_now()}] 🔍 正在定位下载图标...")
        
        # 重新获取iframe句柄
        iframe = self.page.get_frame('.xy-shell__content-frame')
        
        # 等待页面稳定
        time.sleep(2)
        
        download_success = False
        for retry in range(3):
            try:
                icon = iframe.ele('@aria-label=download', timeout=10)
                
                if icon:
                    old_files = os.listdir(self.target_path)
                    self.page.set.download_path(self.target_path)
                    
                    # 定位到父级按钮
                    target_btn = icon.parent('tag:button')
                    
                    # 模拟长按触发下拉菜单
                    self._log(f"[{self._get_now()}] ⏳ 正在模拟长按/悬停触发菜单...")
                    iframe.actions.move_to(target_btn)
                    iframe.actions.hold(target_btn)
                    time.sleep(1.5)
                    iframe.actions.release(target_btn)
                    
                    self._log(f"[{self._get_now()}] ✅ 长按动作完成")
                    
                    # 等待菜单出现
                    menu_item = iframe.wait.ele_displayed('text=导出全量数据', timeout=5)
                    
                    if menu_item:
                        export_start_time = datetime.now()
                        menu_item.click(by_js=True)
                        self._log(f"[{self._get_now()}] 📥 导出指令已发送")
                        time.sleep(3)
                        download_success = True
                        break
                    else:
                        self._log(f"[{self._get_now()}] ⚠️ 长按后下拉菜单仍未显示，尝试第{retry+1}次...")
                else:
                    self._log(f"[{self._get_now()}] ⚠️ 未找到下载按钮图标，尝试第{retry+1}次...")
            except Exception as e:
                self._log(f"[{self._get_now()}] ⚠️ 下载操作失败，尝试第{retry+1}次: {e}")
            
            if not download_success:
                time.sleep(3)
        
        if download_success:
            # 返回这个批次的导出信息，用于后续识别
            return {
                'success': True,
                'export_start_time': export_start_time,
                'batch_info': f"{batch['start']} ~ {batch['end']}"
            }
        else:
            self._log("❌ 下载操作多次失败")
            return {
                'success': False,
                'export_start_time': export_start_time,
                'batch_info': f"{batch['start']} ~ {batch['end']}"
            }

    def _verify_download_files(self, time_windows):
        """验证文件夹中的文件是否包含预期的时间戳（简化输出）"""
        if not os.path.exists(self.target_path):
            self._log(f"[{self._get_now()}] ❌ 下载目录不存在: {self.target_path}")
            return []

        # 获取所有文件
        files = os.listdir(self.target_path)
        if not files:
            self._log(f"[{self._get_now()}] ℹ️ 下载目录为空")
            return []

        # 解析文件名中的时间戳，只关注匹配的文件
        matched_files = []

        # 修复时间窗口比较问题：将所有时间窗口的开始和结束时间都截断到秒级
        # 这样可以与文件名中提取的时间戳精度匹配
        truncated_time_windows = []
        for window_start, window_end in time_windows:
            truncated_window_start = window_start.replace(microsecond=0)
            truncated_window_end = window_end.replace(microsecond=0)
            truncated_time_windows.append((truncated_window_start, truncated_window_end))

        self._log(f"[{self._get_now()}] 🔍 开始验证文件，目录: {self.target_path}")
        self._log(f"[{self._get_now()}] 📅 验证时间窗口: {truncated_time_windows[0][0].strftime('%Y-%m-%d %H:%M:%S')} ~ {truncated_time_windows[0][1].strftime('%Y-%m-%d %H:%M:%S')}")
        self._log(f"[{self._get_now()}] 📋 目录中的文件: {files}")

        for file in files:
            self._log(f"[{self._get_now()}] 📋 检查文件: {file}")
            # 尝试从文件名中提取时间戳
            timestamp_match = re.search(r'(\d{14,17})', file)
            if timestamp_match:
                timestamp_str = timestamp_match.group(1)
                self._log(f"[{self._get_now()}] 📋 提取到时间戳: {timestamp_str}")
                try:
                    # 解析时间戳
                    if len(timestamp_str) >= 14:
                        file_time = datetime.strptime(timestamp_str[:14], '%Y%m%d%H%M%S')
                        if len(timestamp_str) > 14:
                            microsecond_part = timestamp_str[14:]
                            if len(microsecond_part) <= 6:
                                microsecond = int(microsecond_part.ljust(6, '0'))
                                file_time = file_time.replace(microsecond=microsecond)
                    self._log(f"[{self._get_now()}] 📋 解析时间戳为: {file_time}")

                    # 检查是否在预期的时间窗口内（使用截断后的时间窗口）
                    for window_start, window_end in truncated_time_windows:
                        self._log(f"[{self._get_now()}] 📋 检查是否在时间窗口内: {file_time} 是否在 {window_start} 和 {window_end} 之间")
                        if window_start <= file_time <= window_end:
                            matched_files.append(os.path.join(self.target_path, file))
                            self._log(f"[{self._get_now()}] ✅ 文件匹配成功: {file}")
                            break
                        else:
                            self._log(f"[{self._get_now()}] ⏭️ 文件不在时间窗口内: {file}")
                except ValueError as e:
                    self._log(f"[{self._get_now()}] ⚠️ 无法解析时间戳 {timestamp_str}: {e}")
                    continue
            else:
                self._log(f"[{self._get_now()}] ⚠️ 文件名中没有找到时间戳: {file}")

        # 汇总结果（简化输出）
        self._log(f"[{self._get_now()}] 📊 验证结果: 找到 {len(matched_files)} 个匹配文件")

        if matched_files:
            self._log(f"[{self._get_now()}] ✅ 成功下载的文件:")
            for file in matched_files:
                self._log(f"  ✓ {os.path.basename(file)}")

        return matched_files

    def _verify_download_files_with_retry(self, time_windows, expected_rows, max_retries=10, retry_interval=3):
        """轮询验证文件下载情况，直到找到所有预期文件或达到最大重试次数"""
        self._log(f"[{self._get_now()}] 🎯 预期下载文件数: {len(expected_rows)}")
        self._log(f"[{self._get_now()}] 🔄 开始轮询验证，间隔 {retry_interval} 秒，最多重试 {max_retries} 次")
        
        if not expected_rows:
            self._log(f"[{self._get_now()}] ℹ️ 没有预期下载的文件")
            self._verify_download_files(time_windows)
            return
        
        for attempt in range(1, max_retries + 1):
            self._log(f"\n[{self._get_now()}] 🔍 第 {attempt} 次验证检查...")
            
            # 获取当前匹配的文件
            matched_files = self._verify_download_files(time_windows)
            matched_count = len(matched_files)
            expected_count = len(expected_rows)
            
            self._log(f"[{self._get_now()}] 📊 验证结果: {matched_count}/{expected_count} 个文件已找到")
            
            if matched_count >= expected_count:
                self._log(f"[{self._get_now()}] ✅ 所有预期文件都已找到！")
                return matched_files
            elif attempt < max_retries:
                remaining = expected_count - matched_count
                self._log(f"[{self._get_now()}] ⏳ 还有 {remaining} 个文件未找到，{retry_interval} 秒后重试...")
                time.sleep(retry_interval)
            else:
                self._log(f"[{self._get_now()}] ❌ 已达到最大重试次数 ({max_retries})，仍有 {remaining} 个文件未找到")
                break
        
        self._log(f"[{self._get_now()}] 📋 最终验证结果汇总:")
        final_matched = self._verify_download_files(time_windows)
        return final_matched

    def _merge_downloaded_files(self, download_path, date_range_start, date_range_end, task_name=None, matched_files=None):
        """使用Polars合并新下载的文件
        
        参数:
            download_path: 下载目录路径
            date_range_start: 开始日期
            date_range_end: 结束日期
            task_name: 任务名称，用于生成文件名
            matched_files: 已匹配的文件列表，仅合并这些文件
            
        返回值:
            str: 合并文件的路径，合并失败返回None
        """
        if not POLARS_AVAILABLE:
            self._log(f"[{self._get_now()}] ❌ Polars未安装，无法进行文件合并")
            return
        
        if not os.path.exists(download_path):
            self._log(f"[{self._get_now()}] ❌ 下载目录不存在: {download_path}")
            return
        
        # 获取文件列表
        if matched_files and isinstance(matched_files, list):
            # 使用传入的已匹配文件列表
            file_with_timestamps = []
            for file_path in matched_files:
                # 获取文件名
                file = os.path.basename(file_path)
                if not file.endswith('.xlsx'):
                    continue
                
                # 从文件名中提取时间戳
                timestamp = extract_timestamp_from_filename(file)
                if timestamp:
                    file_with_timestamps.append((timestamp, file))
        else:
            # 从目录中获取所有xlsx文件
            files = [f for f in os.listdir(download_path) if f.endswith('.xlsx')]
            if not files:
                self._log(f"[{self._get_now()}] ℹ️ 下载目录中没有找到xlsx文件")
                return
            
            # 检查是否有多个时间段的文件
            file_with_timestamps = []
            for file in files:
                timestamp = extract_timestamp_from_filename(file)
                if timestamp:
                    file_with_timestamps.append((timestamp, file))
        
        if not file_with_timestamps:
            self._log(f"[{self._get_now()}] ⚠️ 没有找到包含有效时间戳的文件")
            return
        
        # 按时间戳排序，时间戳小的文件先合并
        file_with_timestamps.sort(key=lambda x: x[0])
        
        self._log(f"[{self._get_now()}] 📁 找到 {len(file_with_timestamps)} 个文件，按时间戳排序:")
        for timestamp, file in file_with_timestamps:
            self._log(f"  📄 {file} ({timestamp.strftime('%Y-%m-%d %H:%M:%S')})")
        
        # 生成输出文件名（直接使用任务名称，不新增时间戳后缀）
        if task_name:
            # 直接使用任务名称作为文件名
            # 移除文件名中的非法字符
            base_name = task_name
            # 移除或替换非法字符
            base_name = base_name.replace(':', '').replace('/', '').replace('\\', '').replace('*', '').replace('?', '').replace('"', '').replace('<', '').replace('>', '').replace('|', '')
            # 移除任务名称中的日期部分（如果存在）
            name_parts = base_name.split('_')
            if len(name_parts) > 3 and '至' in name_parts[-1]:
                base_name = '_'.join(name_parts[:-2])
            
            output_filename = f"{base_name}.xlsx"
        else:
            output_filename = f"D端-考核_表格.xlsx"
        
        output_path = os.path.join(download_path, output_filename)
        
        try:
            if len(file_with_timestamps) == 1:
                # 单个文件：直接重命名
                self._log(f"[{self._get_now()}] 💾 单个文件，执行重命名操作: {output_filename}")
                self._log(f"[{self._get_now()}] 📍 输出路径: {output_path}")
                
                # 获取原始文件路径
                original_file_path = os.path.join(download_path, file_with_timestamps[0][1])
                
                # 删除已存在的输出文件（如果有）
                if os.path.exists(output_path):
                    os.remove(output_path)
                
                # 重命名文件
                os.rename(original_file_path, output_path)
                self._log(f"[{self._get_now()}] ✅ 文件重命名成功: {output_filename}")
                self._log(f"[{self._get_now()}] 🎉 文件处理完成！")
            else:
                # 多个文件：执行合并操作
                self._log(f"[{self._get_now()}] 💾 多个文件，执行合并操作: {output_filename}")
                self._log(f"[{self._get_now()}] 📍 输出路径: {output_path}")
                
                # 使用Polars读取并合并文件
                dataframes = []
                
                for timestamp, file in file_with_timestamps:
                    file_path = os.path.join(download_path, file)
                    try:
                        self._log(f"[{self._get_now()}] 📖 读取文件: {file}")
                        df = pl.read_excel(file_path)
                        dataframes.append(df)
                        self._log(f"[{self._get_now()}] ✅ 成功读取 {file}，共 {len(df)} 行数据")
                    except Exception as e:
                        self._log(f"[{self._get_now()}] ❌ 读取文件失败 {file}: {e}")
                        continue
                
                if not dataframes:
                    self._log(f"[{self._get_now()}] ❌ 没有成功读取任何文件")
                    return None
                
                # 合并所有数据框
                self._log(f"[{self._get_now()}] 🔄 开始合并 {len(dataframes)} 个文件...")
                try:
                    merged_df = pl.concat(dataframes, how="vertical")
                    self._log(f"[{self._get_now()}] ✅ 文件合并完成，总计 {len(merged_df)} 行数据")
                except Exception as e:
                    self._log(f"[{self._get_now()}] ❌ 文件合并失败: {e}")
                    return None
                
                # 保存合并文件
                merged_df.write_excel(output_path)
                self._log(f"[{self._get_now()}] ✅ 文件保存成功: {output_path}")
                self._log(f"[{self._get_now()}] 📊 合并结果: {len(merged_df)} 行 × {len(merged_df.columns)} 列")
                
                # 删除原始文件
                self._log(f"[{self._get_now()}] 🗑️ 开始删除原始文件...")
                for timestamp, file in file_with_timestamps:
                    file_path = os.path.join(download_path, file)
                    if os.path.exists(file_path) and file_path != output_path:
                        try:
                            os.remove(file_path)
                            self._log(f"[{self._get_now()}] ✅ 删除成功: {file}")
                        except Exception as e:
                            self._log(f"[{self._get_now()}] ❌ 删除失败 {file}: {e}")
                
                self._log(f"[{self._get_now()}] 🎉 文件合并完成！")
        except Exception as e:
            self._log(f"[{self._get_now()}] ❌ 处理文件失败: {e}")
            return None
        
        return output_path  # 返回处理后的文件路径

    def _handle_batch_download(self, export_start_times, start_date, end_date):
        """处理批量下载逻辑"""
        self._log(f"[{self._get_now()}] 🔄 开始批量下载处理...")
        
        # 计算导出时间窗口（从第一个批次开始到最后一个批次结束后30秒）
        if export_start_times:
            # 修复时间窗口计算问题：将开始时间提前2秒，确保所有相关文件都能被包含在内
            export_window_start = min(export_start_times) - timedelta(seconds=2)
            export_window_end = max(export_start_times) + timedelta(seconds=30)
            self._log(f"[{self._get_now()}] 📅 导出时间窗口: {export_window_start.strftime('%Y-%m-%d %H:%M:%S')} ~ {export_window_end.strftime('%Y-%m-%d %H:%M:%S')}")
        else:
            export_window_start = datetime.now() - timedelta(minutes=5)  # 如果没有成功批次，使用最近5分钟
            export_window_end = datetime.now()
            self._log(f"[{self._get_now()}] ⚠️ 没有成功的批次，使用最近5分钟作为时间窗口")
        
        def get_all_rows():
            """获取页面所有下载记录行"""
            return self.page.eles('.self-analysis-download-list-row', timeout=2)

        def download_successful_rows(export_window_start, export_window_end):
            """下载当前导出时间窗口内的成功记录"""
            nonlocal downloaded_rows
            downloaded_this_round = 0
            rows = get_all_rows()
            
            # 修复时间窗口比较问题：将时间窗口的开始和结束时间都截断到秒级
            # 这样可以与页面上显示的秒级时间字符串匹配
            export_window_start = export_window_start.replace(microsecond=0)
            export_window_end = export_window_end.replace(microsecond=0)
            
            for i, row in enumerate(rows):
                try:
                    # 获取任务名、状态和时间
                    filename_ele = row.ele('.self-analysis-download-list-filename', timeout=1)
                    status_ele = row.ele('.self-analysis-download-list-status', timeout=1)
                    time_ele = row.ele('.self-analysis-download-list-time', timeout=1)
                    
                    if not filename_ele or not status_ele or not time_ele:
                        continue
                        
                    task_name = filename_ele.text.strip()
                    status_text = status_ele.text.strip()
                    record_time_str = time_ele.text.strip()
                    
                    self._log(f"[{self._get_now()}] 📋 第{i+1}行: {task_name} | 状态: {status_text} | 时间: {record_time_str}")
                    
                    # 解析记录时间
                    try:
                        record_time = datetime.strptime(record_time_str, '%Y-%m-%d %H:%M:%S')
                    except ValueError:
                        self._log(f"[{self._get_now()}] ⚠️ 无法解析时间格式: {record_time_str}")
                        continue
                    
                    # 检查是否在当前导出时间窗口内
                    is_in_window = export_window_start <= record_time <= export_window_end
                    
                    # 如果状态是成功、该行尚未下载，且在当前导出时间窗口内
                    if "成功" in status_text and i not in downloaded_rows and is_in_window:
                        download_btn = row.ele('text:下载', timeout=1)
                        if download_btn:
                            self._log(f"[{self._get_now()}] 🚀 下载第{i+1}行 (当前批次): {task_name}")
                            download_btn.click()
                            downloaded_rows.add(i)
                            downloaded_this_round += 1
                            time.sleep(1)  # 点击间隔
                        else:
                            self._log(f"[{self._get_now()}] ⚠️ 找到成功状态但未找到下载按钮: {task_name}")
                    elif i in downloaded_rows:
                        self._log(f"[{self._get_now()}] ⏭️ 已下载过第{i+1}行: {task_name}")
                    elif not is_in_window:
                        self._log(f"[{self._get_now()}] ⏭️ 跳过第{i+1}行 (非当前批次): {task_name}")
                    else:
                        self._log(f"[{self._get_now()}] ⏭️ 跳过第{i+1}行 (状态不符): {task_name}")
                    
                except Exception as e:
                    self._log(f"[{self._get_now()}] ⚠️ 处理第{i+1}行时出错: {e}")
                    continue
            
            return downloaded_this_round

        # 主循环：等待并批量下载
        downloaded_rows = set()  # 记录已下载的行索引
        max_retries = 15 
        retry_count = 0 
        
        while retry_count < max_retries: 
            retry_count += 1
            self._log(f"[{self._get_now()}] 🔍 第{retry_count}次检查...")
            
            # 尝试滚动到页面底部以加载更多记录
            try:
                self.page.run_js("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(1)
            except:
                pass
            
            # 下载所有成功的记录
            downloaded_count = download_successful_rows(export_window_start, export_window_end)
            
            if downloaded_count > 0:
                self._log(f"[{self._get_now()}] ✅ 本轮下载了 {downloaded_count} 个文件")
            
            # 检查是否还有未成功的记录
            rows = get_all_rows()
            has_pending = False
            
            for row in rows:
                try:
                    status_ele = row.ele('.self-analysis-download-list-status', timeout=1)
                    if status_ele and "成功" not in status_ele.text.strip():
                        has_pending = True
                        break
                except:
                    continue
            
            if not has_pending and downloaded_count > 0:
                self._log(f"[{self._get_now()}] ✅ 所有记录都已处理完成")
                return downloaded_rows  # 返回实际下载的行索引
            elif not has_pending:
                self._log(f"[{self._get_now()}] ℹ️ 暂无未完成的记录")
                return downloaded_rows  # 返回实际下载的行索引（可能是空的）
            
            # 如果还有未成功的记录，点击刷新
            refresh_btn = self.page.ele('text:刷新') or self.page.ele('.anticon-reload')
            if refresh_btn:
                self._log(f"[{self._get_now()}] 🔄 仍有未完成记录，点击刷新...")
                refresh_btn.click()
                time.sleep(3)
            else:
                self._log(f"[{self._get_now()}] ⏳ 没有找到刷新按钮，等待...")
                time.sleep(3)
        
        if retry_count >= max_retries:
            self._log(f"[{self._get_now()}] ❌ 已达到最大重试次数 ({max_retries})")
            return downloaded_rows  # 返回已下载的行索引，即使达到最大重试次数
        
        return downloaded_rows  # 返回已下载的行索引
        self._log(f"[{self._get_now()}] 📁 输出文件: {output_filename}")
        self._log(f"[{self._get_now()}] 📍 文件路径: {output_path}")

    def quit(self):
        self.page.quit()