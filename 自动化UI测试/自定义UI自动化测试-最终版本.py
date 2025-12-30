import json
import time
import os
from DrissionPage import ChromiumPage, ChromiumOptions
from datetime import datetime, timedelta

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

def process_date_batch(page, iframe, batch, batch_number, total_batches):
    """处理单个日期批次的数据下载"""
    print(f"[{get_now()}] 📅 批次 {batch_number}/{total_batches}: {batch['start']} ~ {batch['end']}")
    
    # 记录导出开始时间
    export_start_time = datetime.now()
    
    # 清空筛选条件
    iframe.run_js("document.querySelectorAll('.filter-item-delete')[3]?.click();")
    time.sleep(1)
    
    # 设置日期范围
    start_input = iframe.ele('@placeholder=开始日期')
    end_input = iframe.ele('@placeholder=结束日期')
    iframe.run_js('arguments[0].removeAttribute("readonly");', start_input)
    iframe.run_js('arguments[0].removeAttribute("readonly");', end_input)
    
    start_input.clear().input(batch['start'])
    end_input.clear().input(batch['end'])
    iframe.actions.key_down('ENTER').key_up('ENTER')
    
    # 分析数据
    iframe.ele('text:开始分析').click()
    print(f"[{get_now()}] 🔍 等待数据查询...")
    iframe.wait.ele_deleted('text:正在努力为您查询', timeout=60)
    time.sleep(3)

    # 下载逻辑
    print(f"[{get_now()}] 🔍 正在定位下载图标...")
    
    # 重新获取iframe句柄
    iframe = page.get_frame('.xy-shell__content-frame')
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
            
            # 返回这个批次的导出信息，用于后续识别
            return {
                'success': True,
                'export_start_time': export_start_time,
                'batch_info': f"{batch['start']} ~ {batch['end']}"
            }
        else:
            print("❌ 长按后下拉菜单仍未显示")
            return {
                'success': False,
                'export_start_time': export_start_time,
                'batch_info': f"{batch['start']} ~ {batch['end']}"
            }
    else:
        print("❌ 未找到下载按钮图标")
        return {
            'success': False,
            'export_start_time': export_start_time,
            'batch_info': f"{batch['start']} ~ {batch['end']}"
        }

def run_task():
    co = ChromiumOptions()
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
        
        # 3. Iframe 捕获
        time.sleep(3)
        iframe = page.get_frame('.xy-shell__content-frame')
        if not iframe:
            print("❌ 未发现 Iframe"); return
        
        iframe.wait.ele_displayed('.filter-item-delete', timeout=20)

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
        
        page.close()

if __name__ == "__main__":
    run_task()