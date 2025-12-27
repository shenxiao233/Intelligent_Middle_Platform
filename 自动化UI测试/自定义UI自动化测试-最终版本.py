import json
import time
import os
from DrissionPage import ChromiumPage, ChromiumOptions
from datetime import datetime

def get_now():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

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

target_dates = {'start': '2025-12-20', 'end': '2025-12-20'}

def self_wait_download(path, old_files):
    print(f"[{get_now()}] 📂 监控下载中...")
    for _ in range(60):
        current_files = os.listdir(path)
        if len(current_files) > len(old_files):
            if not any(f.endswith(('.crdownload', '.tmp')) for f in current_files):
                new_file = list(set(current_files) - set(old_files))[0]
                print(f"[{get_now()}] 🎉 下载成功! 文件名: {new_file}")
                return True
        time.sleep(2)
    return False

def run_task():
    co = ChromiumOptions()
    page = ChromiumPage(co)
    
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

        # 4. 设置筛选
        iframe.run_js("document.querySelectorAll('.filter-item-delete')[3]?.click();")
        
        start_input = iframe.ele('@placeholder=开始日期')
        end_input = iframe.ele('@placeholder=结束日期')
        iframe.run_js('arguments[0].removeAttribute("readonly");', start_input)
        iframe.run_js('arguments[0].removeAttribute("readonly");', end_input)
        
        start_input.clear().input(target_dates['start'])
        end_input.clear().input(target_dates['end'])
        iframe.actions.key_down('ENTER').key_up('ENTER')
        
        # 5. 分析数据
        iframe.ele('text:开始分析').click()
        print(f"[{get_now()}] 🔍 等待数据查询...")
        iframe.wait.ele_deleted('text:正在努力为您查询', timeout=60)
        time.sleep(3)

        # 7. 核心下载逻辑
        print(f"[{get_now()}] 🔍 正在定位下载图标...")
        
        # 重新获取一次句柄防止失效
        iframe = page.get_frame('.xy-shell__content-frame')
        icon = iframe.ele('@aria-label=download', timeout=10)

        if icon:
            old_files = os.listdir(target_path)
            page.set.download_path(target_path)
            
            # 定位到父级按钮
            target_btn = icon.parent('tag:button')
            
            # --- 模拟长按逻辑 ---
            print(f"[{get_now()}] ⏳ 正在模拟长按/悬停触发菜单...")
            # 1. 先把鼠标移动到按钮上
            iframe.actions.move_to(target_btn)
            # 2. 按下鼠标左键不松开
            iframe.actions.hold(target_btn)
            # 3. 停顿 1.5 秒（这就是“长按”效果）
            time.sleep(1.5)
            # 4. 松开鼠标左键
            iframe.actions.release(target_btn)
            
            print(f"[{get_now()}] ✅ 长按动作完成")
            
            # 等待菜单出现（AntD 菜单有时在长按或 Hover 后才会挂载到 DOM）
            menu_item = iframe.wait.ele_displayed('text=导出全量数据', timeout=5)
            
            if menu_item:
                # 菜单项建议使用 JS 点击，穿透力最强
                # menu_item.click(by_js=True)
                print(f"[{get_now()}] 📥 导出指令已发送")
                
                if self_wait_download(target_path, old_files):
                    print(f"[{get_now()}] 🏁 任务全部成功完成！")
                    # --- 这里是新增的跳转逻辑 ---
                    next_url = "https://xy.ele.me/xy-bbjs" # 替换成你想去的页面
                    print(f"[{get_now()}] 🚀 正在跳转到新页面: {next_url}")
                    page.get(next_url)
                

            else:
                print("❌ 长按后下拉菜单仍未显示，尝试直接悬停...")
                # 备选方案：尝试单纯的悬停 (Hover)
                iframe.actions.move_to(target_btn)
                time.sleep(1)
                # 再次尝试寻找
                menu_item_retry = iframe.ele('text=导出全量数据')
                if menu_item_retry:
                    menu_item_retry.click(by_js=True)
        else:
            print("❌ 未找到下载按钮图标")

    except Exception as e:
        print(f"[{get_now()}] ⚠️ 流程由于未知错误中断: {e}")
    finally:
        print(f"[{get_now()}] ⚙️ 浏览器保持运行中...")

if __name__ == "__main__":
    run_task()