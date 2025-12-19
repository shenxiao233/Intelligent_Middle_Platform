# data_processor.py

import pandas as pd
import os
import datetime
import sys

# --- 辅助函数：安全读取 CSV 文件 ---
def safe_read_csv(file_path, required_columns):
    """
    尝试使用 GBK 和 UTF-8 编码安全读取 CSV 文件，并进行基础校验。
    """
    try:
        # 尝试使用 GBK 编码（中国地区 Windows 常用）
        df = pd.read_csv(
            file_path,
            usecols=lambda c: c in required_columns,
            dtype='str',
            na_filter=False,
            encoding='gbk'
        )
        print(f"成功使用 GBK 编码读取文件: {os.path.basename(file_path)}")
        return df
    except UnicodeDecodeError:
        try:
            # 尝试使用 UTF-8 编码（互联网和新系统常用）
            df = pd.read_csv(
                file_path,
                usecols=lambda c: c in required_columns,
                dtype='str',
                na_filter=False,
                encoding='utf-8'
            )
            print(f"成功使用 UTF-8 编码读取文件: {os.path.basename(file_path)}")
            return df
        except Exception as e:
            raise Exception(f"文件编码识别失败，请检查文件格式是否为 GBK 或 UTF-8。原始错误: {str(e)}")
    except pd.errors.ParserError as e:
        raise Exception(f"CSV文件解析错误，可能文件内容不规范或损坏。原始错误: {str(e)}")
    except Exception as e:
        raise Exception(f"读取文件时发生未知错误：{str(e)}")


# --- 核心数据处理逻辑 ---
def process_data_logic(file_path_1, file_path_2, output_dir):
    """
    file_path_1: 有效商户明细
    file_path_2: 商智核/超抢手
    output_dir: 用户选择的输出文件夹路径

    返回：生成的文件的完整路径
    """
    # 确保输出目录存在
    if not os.path.isdir(output_dir):
        os.makedirs(output_dir, exist_ok=True)

    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    output_filename = os.path.join(output_dir, f'商户月汇总_{timestamp}.csv')

    # ================= 处理文件 1：有效商户明细 =================
    try:
        required_columns_1 = ["日期", "商户名称", '总订单', '拼团订单', '非拼团订单', '餐饮营销工具_爆单红包订单']
        df1 = safe_read_csv(file_path_1, required_columns_1)

        if '总订单' not in df1.columns:
            raise ValueError("在文件1中未找到【总订单】列。\n请确认您上传的是【有效商户明细】文件。")

        order_cols = ['总订单', '拼团订单', '非拼团订单', '餐饮营销工具_爆单红包订单']
        for col in order_cols:
            if col in df1.columns:
                df1[col] = pd.to_numeric(df1[col].replace('', '0'), errors='coerce').fillna(0).astype(int)

        daily_summary = df1.groupby('日期').agg({
            '总订单': 'sum',
            '拼团订单': 'sum',
            '非拼团订单': 'sum',
            '餐饮营销工具_爆单红包订单': 'sum'
        }).reset_index().sort_values('日期')

    except Exception as e:
        raise Exception(f"处理【有效商户明细】文件时出错：\n{str(e)}")

    # ================= 处理文件 2：超抢手/商智核 =================
    try:
        required_columns_2 = ["订单日期", "商户id", "商户名称", "业务线", "订单id"]
        df2 = safe_read_csv(file_path_2, required_columns_2)

        if '订单日期' not in df2.columns or '订单id' not in df2.columns:
            raise ValueError("在文件2中未找到【订单日期】或【订单id】。\n请确认您上传的是【超抢手/商智核】文件。")

        df2 = df2[[c for c in required_columns_2 if c in df2.columns]]

        daily_orders = df2.groupby('订单日期')['订单id'].count().reset_index()
        daily_orders.columns = ['日期', '超抢手订单']
        daily_orders = daily_orders.sort_values('日期')

    except Exception as e:
        raise Exception(f"处理【超抢手/商智核】文件时出错：\n{str(e)}")

    # ================= 合并与计算 =================
    try:
        merged_data = pd.merge(daily_summary, daily_orders, on='日期', how='outer', sort=True)

        cols_to_fix = ['总订单', '拼团订单', '非拼团订单', '餐饮营销工具_爆单红包订单', '超抢手订单']
        for col in cols_to_fix:
            if col in merged_data.columns:
                merged_data[col] = merged_data[col].fillna(0).astype(int)
            else:
                merged_data[col] = 0

        def calculate_percentage_with_symbol(numerator, denominator):
            if denominator == 0:
                return "0.00%"
            percentage = (numerator / denominator) * 100
            return f"{percentage:.2f}%"

        merged_data['拼团订单占比(%)'] = merged_data.apply(
            lambda row: calculate_percentage_with_symbol(row['拼团订单'], row['总订单']), axis=1)
        merged_data['非拼团订单占比(%)'] = merged_data.apply(
            lambda row: calculate_percentage_with_symbol(row['非拼团订单'], row['总订单']), axis=1)
        merged_data['爆单红包订单占比(%)'] = merged_data.apply(
            lambda row: calculate_percentage_with_symbol(row['餐饮营销工具_爆单红包订单'], row['总订单']), axis=1)
        merged_data['超抢手订单占比(%)'] = merged_data.apply(
            lambda row: calculate_percentage_with_symbol(row['超抢手订单'], row['总订单']), axis=1)

        final_cols = [
            '日期', '总订单', '拼团订单', '非拼团订单',
            '餐饮营销工具_爆单红包订单', '超抢手订单',
            '拼团订单占比(%)', '非拼团订单占比(%)',
            '爆单红包订单占比(%)', '超抢手订单占比(%)'
        ]

        merged_data_filtered = merged_data[[c for c in final_cols if c in merged_data.columns]].copy()

        # 写入文件，使用GBK编码确保兼容Windows Excel
        merged_data_filtered.to_csv(output_filename, index=False, encoding='gbk')

        return output_filename

    except Exception as e:
        print(f"合并数据或计算时出错：{e}", file=sys.stderr)
        raise Exception(f"合并数据或计算时出错：\n{str(e)}")