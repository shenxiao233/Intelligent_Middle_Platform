import json
import time
import os
import re
from DrissionPage import ChromiumPage, ChromiumOptions
from datetime import datetime, timedelta

# 尝试导入polars，如果不存在则提示安装
try:
    import polars as pl
    POLARS_AVAILABLE = True
except ImportError:
    POLARS_AVAILABLE = False
    print(f"[{get_now()}] ⚠️ Polars未安装，无法进行文件合并功能")

def get_now():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

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

COOKIE_JSON_RAW = """
[
    {
        "domain": ".ele.me",
        "name": "AEOLUS_MOZI_TOKEN",
        "value": "PBE_2.0_5221922713280a41ccb593784a3305b9708b6c19f5b7ea97bdb663cbe871430d009d9edc812e1f60ca101398278b3403927a54720a1e63f0dae2d684f7dbfb5bb0d05ad43031ad903f268efd0bec9bccdf964fb578c04388bef3ad422c19ea6f502bbfe742b531db11667db56d955317154296d368763862c8b5aac729a25e81ab6adbe6fbd10644637f561cfea6e47aba7143eee4f43c88e57f0649a2fa9d3256fbb1c65001af7d343de020981d7a347dd1f4ffd587078b20940ac091a2cd7bfc5a1511fe87c41ceea04da26d0eed0f"
    },
    {
        "domain": ".ele.me",
        "name": "family",
        "value": "xy_token:FAMILY:NORMAL:NZIWYMMTAwMDExODEzNjY3OTAzUGxGYWlMSTdQ"
    },
    {
        "domain": ".ele.me",
        "name": "XY_TOKEN",
        "value": "xy_token:FAMILY:NORMAL:NZIWYMMTAwMDExODEzNjY3OTAzUGxGYWlMSTdQ"
    }
]
"""

target_path = os.path.join(os.getcwd(), 'mydata')
if not os.path.exists(target_path):
    os.makedirs(target_path)

target_dates = {'start': '2025-12-15', 'end': '2025-12-25'}

def verify_download_files(download_path, time_windows):
    """验证文件夹中的文件是否包含预期的时间戳（简化输出）"""
    if not os.path.exists(download_path):
        print(f"[{get_now()}] ❌ 下载目录不存在: {download_path}")
        return []

    # 获取所有文件
    files = os.listdir(download_path)
    if not files:
        print(f"[{get_now()}] ℹ️ 下载目录为空")
        return []

    # 解析文件名中的时间戳，只关注匹配的文件
    matched_files = []

    for file in files:
        # 尝试从文件名中提取时间戳
        timestamp_match = re.search(r'(\d{14,17})', file)
        if timestamp_match:
            timestamp_str = timestamp_match.group(1)
            try:
                # 解析时间戳
                if len(timestamp_str) >= 14:
                    file_time = datetime.strptime(timestamp_str[:14], '%Y%m%d%H%M%S')
                    if len(timestamp_str) > 14:
                        microsecond_part = timestamp_str[14:]
                        if len(microsecond_part) <= 6:
                            microsecond = int(microsecond_part.ljust(6, '0'))
                            file_time = file_time.replace(microsecond=microsecond)

                    # 检查是否在预期的时间窗口内
                    for window_start, window_end in time_windows:
                        if window_start <= file_time <= window_end:
                            matched_files.append(file)
                            break
            except ValueError:
                # 忽略无法解析的文件
                continue

    # 汇总结果（简化输出）
    print(f"[{get_now()}] 📊 验证结果: 找到 {len(matched_files)} 个匹配文件")

    if matched_files:
        print(f"[{get_now()}] ✅ 成功下载的文件:")
        for file in matched_files:
            print(f"  ✓ {file}")

    return matched_files

def verify_download_files_with_retry(download_path, time_windows, expected_rows, max_retries=10, retry_interval=3):
    """轮询验证文件下载情况，直到找到所有预期文件或达到最大重试次数"""
    print(f"[{get_now()}] 🎯 预期下载文件数: {len(expected_rows)}")
    print(f"[{get_now()}] 🔄 开始轮询验证，间隔 {retry_interval} 秒，最多重试 {max_retries} 次")
    
    if not expected_rows:
        print(f"[{get_now()}] ℹ️ 没有预期下载的文件")
        verify_download_files(download_path, time_windows)
        return
    
    for attempt in range(1, max_retries + 1):
        print(f"\n[{get_now()}] 🔍 第 {attempt} 次验证检查...")
        
        # 获取当前匹配的文件
        matched_files = verify_download_files(download_path, time_windows)
        matched_count = len(matched_files)
        expected_count = len(expected_rows)
        
        print(f"[{get_now()}] 📊 验证结果: {matched_count}/{expected_count} 个文件已找到")
        
        if matched_count >= expected_count:
            print(f"[{get_now()}] ✅ 所有预期文件都已找到！")
            return matched_files
        elif attempt < max_retries:
            remaining = expected_count - matched_count
            print(f"[{get_now()}] ⏳ 还有 {remaining} 个文件未找到，{retry_interval} 秒后重试...")
            time.sleep(retry_interval)
        else:
            print(f"[{get_now()}] ❌ 已达到最大重试次数 ({max_retries})，仍有 {remaining} 个文件未找到")
            break
    
    print(f"[{get_now()}] 📋 最终验证结果汇总:")
    final_matched = verify_download_files(download_path, time_windows)
    return final_matched

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

def merge_downloaded_files(download_path, date_range_start, date_range_end):
    """使用Polars合并新下载的文件"""
    if not os.path.exists(download_path):
        print(f"[{get_now()}] ❌ 下载目录不存在: {download_path}")
        return
    
    # 获取所有xlsx文件
    files = [f for f in os.listdir(download_path) if f.endswith('.xlsx')]
    if not files:
        print(f"[{get_now()}] ℹ️ 下载目录中没有找到xlsx文件")
        return
    
    # 按时间戳排序文件（从最早的开始）
    file_with_timestamps = []
    for file in files:
        timestamp = extract_timestamp_from_filename(file)
        if timestamp:
            file_with_timestamps.append((timestamp, file))
    
    if not file_with_timestamps:
        print(f"[{get_now()}] ⚠️ 没有找到包含有效时间戳的文件")
        return
    
    # 按时间戳排序
    file_with_timestamps.sort(key=lambda x: x[0])
    
    print(f"[{get_now()}] 📁 找到 {len(file_with_timestamps)} 个文件，按时间戳排序:")
    for timestamp, file in file_with_timestamps:
        print(f"  📄 {file} ({timestamp.strftime('%Y-%m-%d %H:%M:%S')})")
    
    # 使用Polars读取并合并文件
    dataframes = []
    
    for timestamp, file in file_with_timestamps:
        file_path = os.path.join(download_path, file)
        try:
            print(f"[{get_now()}] 📖 读取文件: {file}")
            df = pl.read_excel(file_path)
            dataframes.append(df)
            print(f"[{get_now()}] ✅ 成功读取 {file}，共 {len(df)} 行数据")
        except Exception as e:
            print(f"[{get_now()}] ❌ 读取文件失败 {file}: {e}")
            continue
    
    if not dataframes:
        print(f"[{get_now()}] ❌ 没有成功读取任何文件")
        return
    
    # 合并所有数据框
    print(f"[{get_now()}] 🔄 开始合并 {len(dataframes)} 个文件...")
    try:
        merged_df = pl.concat(dataframes, how="vertical")
        print(f"[{get_now()}] ✅ 文件合并完成，总计 {len(merged_df)} 行数据")
    except Exception as e:
        print(f"[{get_now()}] ❌ 文件合并失败: {e}")
        return
    
    # 生成输出文件名（添加日期范围后缀）
    date_suffix = f"{date_range_start.replace('-', '')}至{date_range_end.replace('-', '')}"
    output_filename = f"D端-考核_表格_合并_{date_suffix}.xlsx"
    output_path = os.path.join(download_path, output_filename)
    
    try:
        print(f"[{get_now()}] 💾 保存合并文件: {output_filename}")
        merged_df.write_excel(output_path)
        print(f"[{get_now()}] ✅ 文件保存成功: {output_path}")
        print(f"[{get_now()}] 📊 合并结果: {len(merged_df)} 行 × {len(merged_df.columns)} 列")
    except Exception as e:
        print(f"[{get_now()}] ❌ 保存文件失败: {e}")
        return
    
    print(f"[{get_now()}] 🎉 文件合并完成！")
    print(f"[{get_now()}] 📁 输出文件: {output_filename}")
    print(f"[{get_now()}] 📍 文件路径: {output_path}")

def process_date_batch(page, iframe, batch, batch_number, total_batches):
    """处理单个日期批次的数据下载"""
    print(f"[{get_now()}] 📅 批次 {batch_number}/{total_batches}: {batch['start']} ~ {batch['end']}")
    
    # 记录导出开始时间
    export_start_time = datetime.now()
    
    try:
        # 重新获取iframe句柄，确保是最新的
        iframe = page.get_frame('.xy-shell__content-frame')
        
        # 等待页面稳定
        time.sleep(2)
        
        # 1. 清空筛选条件 - 增加重试机制
        print(f"[{get_now()}] 🧹 清空筛选条件...")
        clear_success = False
        for retry in range(3):
            try:
                clear_btns = iframe.eles('.filter-item-delete', timeout=3)
                if len(clear_btns) >= 4:
                    clear_btns[3].click()
                    clear_success = True
                    print(f"[{get_now()}] ✅ 成功清空筛选条件")
                    break
                else:
                    print(f"[{get_now()}] ⚠️ 清空按钮不足，尝试第{retry+1}次...")
            except Exception as e:
                print(f"[{get_now()}] ⚠️ 清空操作失败，尝试第{retry+1}次: {e}")
            
            if not clear_success:
                time.sleep(1)
        
        if not clear_success:
            print(f"[{get_now()}] ⚠️ 清空筛选条件失败，但继续执行...")
        
        time.sleep(1)
        
        # 2. 设置日期范围 - 增加重试机制
        print(f"[{get_now()}] 📅 设置日期范围...")
        date_set_success = False
        for retry in range(3):
            try:
                start_input = iframe.ele('@placeholder=开始日期', timeout=5)
                end_input = iframe.ele('@placeholder=结束日期', timeout=5)
                
                if start_input and end_input:
                    # 移除只读属性
                    iframe.run_js('arguments[0].removeAttribute("readonly");', start_input)
                    iframe.run_js('arguments[0].removeAttribute("readonly");', end_input)
                    
                    # 清空并输入日期
                    start_input.clear().input(batch['start'])
                    end_input.clear().input(batch['end'])
                    
                    # 按回车确认
                    iframe.actions.key_down('ENTER').key_up('ENTER')
                    
                    date_set_success = True
                    print(f"[{get_now()}] ✅ 成功设置日期范围: {batch['start']} ~ {batch['end']}")
                    break
                else:
                    print(f"[{get_now()}] ⚠️ 日期输入框未找到，尝试第{retry+1}次...")
            except Exception as e:
                print(f"[{get_now()}] ⚠️ 设置日期失败，尝试第{retry+1}次: {e}")
            
            if not date_set_success:
                time.sleep(2)
        
        if not date_set_success:
            print(f"[{get_now()}] ❌ 设置日期范围失败")
            return {
                'success': False,
                'export_start_time': export_start_time,
                'batch_info': f"{batch['start']} ~ {batch['end']}"
            }
        
        # 3. 开始分析 - 增加重试机制
        print(f"[{get_now()}] 🔍 点击开始分析...")
        analysis_success = False
        for retry in range(3):
            try:
                analyze_btn = iframe.ele('text:开始分析', timeout=5)
                if analyze_btn:
                    analyze_btn.click()
                    analysis_success = True
                    print(f"[{get_now()}] ✅ 成功点击开始分析")
                    break
                else:
                    print(f"[{get_now()}] ⚠️ 开始分析按钮未找到，尝试第{retry+1}次...")
            except Exception as e:
                print(f"[{get_now()}] ⚠️ 点击开始分析失败，尝试第{retry+1}次: {e}")
            
            if not analysis_success:
                time.sleep(2)
        
        if not analysis_success:
            print(f"[{get_now()}] ❌ 点击开始分析失败")
            return {
                'success': False,
                'export_start_time': export_start_time,
                'batch_info': f"{batch['start']} ~ {batch['end']}"
            }
        
        print(f"[{get_now()}] 🔍 等待数据查询...")
        try:
            iframe.wait.ele_deleted('text:正在努力为您查询', timeout=60)
            print(f"[{get_now()}] ✅ 数据查询完成")
        except:
            print(f"[{get_now()}] ⚠️ 查询完成状态检查超时，但继续执行...")
        
        time.sleep(3)

        # 4. 下载逻辑 - 增加重试机制
        print(f"[{get_now()}] 🔍 正在定位下载图标...")
        
        # 重新获取iframe句柄
        iframe = page.get_frame('.xy-shell__content-frame')
        
        # 等待页面稳定
        time.sleep(2)
        
        download_success = False
        for retry in range(3):
            try:
                icon = iframe.ele('@aria-label=download', timeout=10)
                
                if icon:
                    old_files = os.listdir(target_path)
                    page.set.download_path(target_path)
                    
                    # 定位到父级按钮
                    target_btn = icon.parent('tag:button')
                    
                    # 模拟长按触发下拉菜单
                    print(f"[{get_now()}] ⏳ 正在模拟长按/悬停触发菜单...")
                    iframe.actions.move_to(target_btn)
                    iframe.actions.hold(target_btn)
                    time.sleep(1.5)
                    iframe.actions.release(target_btn)
                    
                    print(f"[{get_now()}] ✅ 长按动作完成")
                    
                    # 等待菜单出现
                    menu_item = iframe.wait.ele_displayed('text=导出全量数据', timeout=5)
                    
                    if menu_item:
                        menu_item.click(by_js=True)
                        print(f"[{get_now()}] 📥 导出指令已发送")
                        time.sleep(3)
                        download_success = True
                        break
                    else:
                        print(f"[{get_now()}] ⚠️ 长按后下拉菜单仍未显示，尝试第{retry+1}次...")
                else:
                    print(f"[{get_now()}] ⚠️ 未找到下载按钮图标，尝试第{retry+1}次...")
            except Exception as e:
                print(f"[{get_now()}] ⚠️ 下载操作失败，尝试第{retry+1}次: {e}")
            
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
            print("❌ 下载操作多次失败")
            return {
                'success': False,
                'export_start_time': export_start_time,
                'batch_info': f"{batch['start']} ~ {batch['end']}"
            }
            
    except Exception as e:
        print(f"[{get_now()}] ❌ 批次处理过程中出现异常: {e}")
        return {
            'success': False,
            'export_start_time': export_start_time,
            'batch_info': f"{batch['start']} ~ {batch['end']}"
        }

def run_task():
    co = ChromiumOptions()
    # 设置浏览器启动时最大化
    co.set_argument('--start-maximized')
    page = ChromiumPage(co)
    
    # 初始化变量，确保在所有情况下都可用
    batch_exports = []
    export_start_times = []
    
    try:
        print(f"[{get_now()}] 🔄 启动任务流程...")
        
        # 1. 登录与 Cookie 验证
        page.get("https://xy.ele.me")
        raw_str = COOKIE_JSON_RAW.strip()
        
        if not raw_str.startswith('[') or '...' in raw_str:
            print(f"[{get_now()}] ❌ 报错停止：Cookie 内容不完整或格式错误。")
            return

        cookies = json.loads(raw_str)
        for cookie in cookies:
            page.set.cookies(cookie)
        print(f"[{get_now()}] ✅ Cookie 注入成功")

        # 2. 访问分析页
        target_url = "https://xy.ele.me/xy-zzfx?url=https%3A%2F%2Fradar360.faas.ele.me%2Fxy%2Fdata-center%2Fself-analysis%3FversionCode%3Dcd-data-center%26appId%3DAUTO_ARK_3gw8GqJPF%26spaceId%3D140%26analysisCode%3DlNuxr%26recordId%3D150776"
        page.get(target_url)
        
        # 等待页面完全加载
        print(f"[{get_now()}] ⏳ 等待页面完全加载...")
        time.sleep(5)
        
        # 检查页面是否完全加载
        try:
            page.wait.load_complete(timeout=10)
            print(f"[{get_now()}] ✅ 页面加载完成")
        except:
            print(f"[{get_now()}] ⚠️ 页面加载状态检查失败，继续执行...")
        
        # 3. Iframe 捕获 - 增加重试机制
        iframe = None
        max_retry = 3
        for retry in range(max_retry):
            print(f"[{get_now()}] 🔍 尝试获取iframe (第{retry+1}次)...")
            time.sleep(2)
            iframe = page.get_frame('.xy-shell__content-frame')
            if iframe:
                print(f"[{get_now()}] ✅ 成功获取iframe")
                break
            else:
                print(f"[{get_now()}] ⚠️ 第{retry+1}次未找到iframe，{3-retry}次后重试...")
                time.sleep(2)
        
        if not iframe:
            print("❌ 多次尝试后仍未发现 Iframe，可能页面加载有问题")
            return
        
        # 等待iframe内容加载
        print(f"[{get_now()}] ⏳ 等待iframe内容加载...")
        time.sleep(3)
        
        # 尝试等待关键元素出现
        try:
            print(f"[{get_now()}] 🔍 检查关键元素是否加载...")
            iframe.wait.ele_displayed('.filter-item-delete', timeout=15)
            print(f"[{get_now()}] ✅ 关键元素已加载")
        except:
            print(f"[{get_now()}] ⚠️ 关键元素加载超时，但继续执行...")
            # 检查页面是否有内容
            try:
                # 尝试等待页面有内容出现
                iframe.wait.ele_displayed('body', timeout=5)
                print(f"[{get_now()}] ✅ 页面基本元素已加载")
            except:
                print(f"[{get_now()}] ❌ 页面加载可能存在问题")

        # 分割日期范围
        date_batches = split_date_range(target_dates['start'], target_dates['end'], max_days=8)
        print(f"[{get_now()}] 📊 共分割为 {len(date_batches)} 个批次")
        
        # 处理每个批次
        for i, batch in enumerate(date_batches, 1):
            batch_result = process_date_batch(page, iframe, batch, i, len(date_batches))
            batch_exports.append(batch_result)
            
            if not batch_result['success']:
                print(f"[{get_now()}] ⚠️ 批次 {i} 处理失败，跳过此批次")
            
            # 批次间等待
            if i < len(date_batches):
                print(f"[{get_now()}] ⏳ 等待 3 秒后进行下一批次...")
                time.sleep(3)
        
        # 获取所有批次的导出开始时间，用于筛选当前导出记录
        export_start_times = [batch['export_start_time'] for batch in batch_exports if batch['success']]
        print(f"[{get_now()}] 📝 记录了 {len(export_start_times)} 个批次的导出开始时间")

    except Exception as e:
        print(f"[{get_now()}] ⚠️ 流程由于未知错误中断: {e}")
    finally:
        # --- 这里是新增的跳转逻辑 ---
        next_url = "https://xy.ele.me/xy-bbjs" # 替换成你想去的页面
        print(f"[{get_now()}] 🚀 正在跳转到新页面: {next_url}")
        page.get(next_url)
        # --- 重要：点击“下载记录”---
        print(f"[{get_now()}] 🔍 正在打开“下载记录”列表...")
        record_btn = page.ele('text:下载记录') or page.ele('@data-spm-anchor-id')
        if record_btn:
            record_btn.click()
            time.sleep(3)

        # --- 5. 批量下载逻辑 --- 
        max_retries = 15 
        retry_count = 0 
        downloaded_rows = set()  # 记录已下载的行索引
        
        # 计算导出时间窗口（从第一个批次开始到最后一个批次结束后30秒）
        if export_start_times:
            export_window_start = min(export_start_times)
            export_window_end = max(export_start_times) + timedelta(seconds=30)
            print(f"[{get_now()}] 📅 导出时间窗口: {export_window_start.strftime('%Y-%m-%d %H:%M:%S')} ~ {export_window_end.strftime('%Y-%m-%d %H:%M:%S')}")
        else:
            export_window_start = datetime.now() - timedelta(minutes=5)  # 如果没有成功批次，使用最近5分钟
            export_window_end = datetime.now()
            print(f"[{get_now()}] ⚠️ 没有成功的批次，使用最近5分钟作为时间窗口")
        
        def get_all_rows():
            """获取页面所有下载记录行"""
            return page.eles('.self-analysis-download-list-row', timeout=2)

        def download_successful_rows(export_window_start, export_window_end):
            """下载当前导出时间窗口内的成功记录"""
            nonlocal downloaded_rows
            downloaded_this_round = 0
            rows = get_all_rows()
            
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
                    
                    print(f"[{get_now()}] 📋 第{i+1}行: {task_name} | 状态: {status_text} | 时间: {record_time_str}")
                    
                    # 解析记录时间
                    try:
                        record_time = datetime.strptime(record_time_str, '%Y-%m-%d %H:%M:%S')
                    except ValueError:
                        print(f"[{get_now()}] ⚠️ 无法解析时间格式: {record_time_str}")
                        continue
                    
                    # 检查是否在当前导出时间窗口内
                    is_in_window = export_window_start <= record_time <= export_window_end
                    
                    # 如果状态是成功、该行尚未下载，且在当前导出时间窗口内
                    if "成功" in status_text and i not in downloaded_rows and is_in_window:
                        download_btn = row.ele('text:下载', timeout=1)
                        if download_btn:
                            print(f"[{get_now()}] 🚀 下载第{i+1}行 (当前批次): {task_name}")
                            download_btn.click()
                            downloaded_rows.add(i)
                            downloaded_this_round += 1
                            time.sleep(1)  # 点击间隔
                        else:
                            print(f"[{get_now()}] ⚠️ 找到成功状态但未找到下载按钮: {task_name}")
                    elif i in downloaded_rows:
                        print(f"[{get_now()}] ⏭️ 已下载过第{i+1}行: {task_name}")
                    elif not is_in_window:
                        print(f"[{get_now()}] ⏭️ 跳过第{i+1}行 (非当前批次): {task_name}")
                    else:
                        print(f"[{get_now()}] ⏭️ 跳过第{i+1}行 (状态不符): {task_name}")
                    
                except Exception as e:
                    print(f"[{get_now()}] ⚠️ 处理第{i+1}行时出错: {e}")
                    continue
            
            return downloaded_this_round

        # 主循环：等待并批量下载
        while retry_count < max_retries: 
            retry_count += 1
            print(f"[{get_now()}] 🔍 第{retry_count}次检查...")
            
            # 尝试滚动到页面底部以加载更多记录
            try:
                page.run_js("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(1)
            except:
                pass
            
            # 下载所有成功的记录
            downloaded_count = download_successful_rows(export_window_start, export_window_end)
            
            if downloaded_count > 0:
                print(f"[{get_now()}] ✅ 本轮下载了 {downloaded_count} 个文件")
            
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
                print(f"[{get_now()}] ✅ 所有记录都已处理完成")
                break
            elif not has_pending:
                print(f"[{get_now()}] ℹ️ 暂无未完成的记录")
                break
            
            # 如果还有未成功的记录，点击刷新
            refresh_btn = page.ele('text:刷新') or page.ele('.anticon-reload')
            if refresh_btn:
                print(f"[{get_now()}] 🔄 仍有未完成记录，点击刷新...")
                refresh_btn.click()
                time.sleep(3)
            else:
                print(f"[{get_now()}] ⚠️ 未找到刷新按钮")
                time.sleep(2)
                
            # 如果达到最大重试次数
            if retry_count >= max_retries:
                print(f"[{get_now()}] ❌ 已达到最大重试次数({max_retries})，停止检查")
                break
        
        print(f"[{get_now()}] 🎉 批量下载完成！共下载了 {len(downloaded_rows)} 个文件")
        print(f"[{get_now()}] 📅 导出时间窗口: {export_window_start.strftime('%Y-%m-%d %H:%M:%S')} ~ {export_window_end.strftime('%Y-%m-%d %H:%M:%S')}")
        if downloaded_rows:
            print(f"[{get_now()}] 📁 已下载记录行:")
            for row_index in sorted(downloaded_rows):
                print(f"  - 第{row_index+1}行")
        else:
            print(f"[{get_now()}] ℹ️ 本次没有下载任何文件")

        # --- 6. 文件时间戳验证 ---
        print(f"[{get_now()}] 🔍 开始验证文件下载情况...")
        # 使用实际的导出时间窗口（从第一个批次开始到最后一个批次结束后30秒）
        if export_start_times:
            actual_window_start = min(export_start_times)
            actual_window_end = max(export_start_times) + timedelta(seconds=30)
            time_windows = [(actual_window_start, actual_window_end)]
            print(f"[{get_now()}] 📅 文件验证时间窗口: {actual_window_start.strftime('%Y-%m-%d %H:%M:%S')} ~ {actual_window_end.strftime('%Y-%m-%d %H:%M:%S')}")
        else:
            time_windows = []
        
        # 轮询验证文件下载情况
        verify_download_files_with_retry(target_path, time_windows, downloaded_rows)

        # --- 7. 使用Polars合并新下载的文件 ---
        if POLARS_AVAILABLE and downloaded_rows:
            print(f"[{get_now()}] 📊 开始使用Polars合并新下载的文件...")
            merge_downloaded_files(target_path, target_dates['start'], target_dates['end'])
        elif not POLARS_AVAILABLE:
            print(f"[{get_now()}] ℹ️ Polars未安装，跳过文件合并功能")
        else:
            print(f"[{get_now()}] ℹ️ 没有下载的文件，跳过合并功能")

        page.close()

if __name__ == "__main__":
    run_task()