# data_worker.py

import pandas as pd
import numpy as np
from typing import List, Optional
from datetime import datetime
import glob
import os
from PySide6.QtCore import QRunnable, QObject, Signal, Slot


# =========================================================================
# 封装您的原始 CSV 合并函数 (保持不变)
# =========================================================================

def merge_csv_and_return_df(
        base_folder_path: str,
        file_prefix: str,
        required_columns: List[str]
) -> Optional[pd.DataFrame]:
    search_pattern = os.path.join(base_folder_path, f'{file_prefix}*.csv')
    csv_files = glob.glob(search_pattern)
    if not csv_files:
        return None

    all_results = []
    for file_path in csv_files:
        try:
            try:
                df = pd.read_csv(file_path, encoding='gbk')
            except UnicodeDecodeError:
                df = pd.read_csv(file_path, encoding='utf-8')
            except Exception:
                continue
            existing_columns = [col for col in required_columns if col in df.columns]
            if not existing_columns:
                continue
            all_results.append(df[existing_columns].copy())
        except Exception:
            pass
    return pd.concat(all_results, ignore_index=True) if all_results else None


# =========================================================================
# Worker 信号和类
# =========================================================================

class WorkerSignals(QObject):
    finished = Signal()
    error = Signal(str)
    result = Signal(pd.DataFrame)
    progress = Signal(str)


class DataWorker(QRunnable):
    def __init__(self, output_path: str, base_path_daily: str, file_prefix_daily: str, base_path_baodan: str,
                 file_prefix_baodan: str):
        super().__init__()
        self.signals = WorkerSignals()
        self.output_path = output_path
        self.BASE_PATH_DAILY = base_path_daily
        self.FILE_PREFIX_DAILY = file_prefix_daily
        self.BASE_PATH_BAODAN = base_path_baodan
        self.FILE_PREFIX_BAODAN = file_prefix_baodan

        # 字段列表定义 (保持您提供的顺序)
        self.REQUIRED_COLS_DAILY = [
            '日期', '商户id', '商户名称', 'bd名称', '区县名称', '区县名称_合并前',
            '是否有营业执照', '是否有经营许可证', '业务线含tn', '主营类目', '品牌id',
            '品牌', '商户审核通过日期', '专送商圈名称', '网格名称', '蜂窝名称',
            '商户整体服务包标签', '零售事业部', '餐饮一点五级类目', '当日是否营业',
            '总订单', '毛g', '净g', '餐饮营销工具_爆单红包订单', '代理商总补贴金额',
            '蜂巢名称', 'retailer_id', '净笔单30及以上订单数',
            '满减拆分_首档满减_力度', '满减拆分_首档满减_商补', '满减拆分_首档满减_饿补',
            '满减拆分_首档满减_代补', '满减拆分_二档满减_力度', '满减拆分_二档满减_商补',
            '满减拆分_二档满减_饿补', '满减拆分_二档满减_代补', '满减拆分_三档满减_力度',
            '满减拆分_三档满减_商补', '满减拆分_三档满减_饿补', '满减拆分_三档满减_代补',
            'c配拆分_减免金额', 'c配拆分_活动配置三方饿补', 'c配拆分_活动配置三方代补',
            'c配拆分_活动配置三方商补'
        ]
        self.REQUIRED_COLS_BAODAN = [
            '日期', 'shop_id', '代理商付费用户加码订单量', '代理商非付费用户加码订单量',
            '代理商付费用户加码金额', '代理商非付费用户加码金额', '阶梯A1报名配置金额',
            '阶梯A2报名配置金额', '阶梯A3报名配置金额', 'A1代理商降门槛金额',
            'A2代理商降门槛金额', 'A3代理商降门槛金额'
        ]
        self.REQUIRED_COLUMNS_ORDER = [
            '日期', '商户id', '商户名称', 'retailer_id', '区县名称_合并前',
            '区县名称', '蜂巢名称', 'bd名称', '网格名称', '蜂窝名称',
            '专送商圈名称', '品牌id', '品牌', '零售事业部', '主营类目',
            '餐饮一点五级类目', '业务线含tn', '商户整体服务包标签', '商户审核通过日期', '当日是否营业',
            '毛g', '净g', '总订单', '是否有营业执照', '是否有经营许可证',
            '净笔单30及以上订单数', '餐饮营销工具_爆单红包订单', '代理商总补贴金额', '代理商付费用户加码订单量',
            '代理商非付费用户加码订单量',
            '代理商非付费用户加码金额', '代理商付费用户加码金额',
            '近30日总订单', '近30日毛g', '近30日净g', '近30日营业天数', '近30天净笔单30+订单数', '近30日爆单订单',
            '当月累计总订单', '当月累计毛g', '当月累计净g', '当月累计营业天数', '当月爆单红包订单',
            '当月代理商非付费用户加码订单量', '当月代理商付费用户加码订单量', '当月代理商非付费用户加码金额',
            '当月代理商付费用户加码金额', '当月代理商总补贴金额',
            '满减拆分_首档满减_力度', '满减拆分_首档满减_商补', '满减拆分_首档满减_饿补', '满减拆分_首档满减_代补',
            '满减拆分_二档满减_力度', '满减拆分_二档满减_商补', '满减拆分_二档满减_饿补', '满减拆分_二档满减_代补',
            '满减拆分_三档满减_力度', '满减拆分_三档满减_商补', '满减拆分_三档满减_饿补', '满减拆分_三档满减_代补',
            'c配拆分_减免金额', 'c配拆分_活动配置三方饿补', 'c配拆分_活动配置三方代补', 'c配拆分_活动配置三方商补',
            '阶梯A1报名配置金额', '阶梯A2报名配置金额', '阶梯A3报名配置金额', 'A1代理商降门槛金额',
            'A2代理商降门槛金额', 'A3代理商降门槛金额'
        ]

    @Slot()
    def run(self):
        self.signals.progress.emit("🚀 任务启动：正在读取并合并 CSV 文件...")
        try:
            # 1. 读取并初步合并
            merged_daily = merge_csv_and_return_df(self.BASE_PATH_DAILY, self.FILE_PREFIX_DAILY,
                                                   self.REQUIRED_COLS_DAILY)
            if merged_daily is None or merged_daily.empty:
                raise ValueError("主表无数据，请检查路径和前缀。")

            merged_baodan = merge_csv_and_return_df(self.BASE_PATH_BAODAN, self.FILE_PREFIX_BAODAN,
                                                    self.REQUIRED_COLS_BAODAN)

            # 2. 执行 Left Merge
            self.signals.progress.emit("🔗 正在执行多表关联...")
            if merged_baodan is not None and not merged_baodan.empty:
                merged_df = pd.merge(
                    left=merged_daily, right=merged_baodan,
                    left_on=['日期', '商户id'], right_on=['日期', 'shop_id'],
                    how='left'
                )
                if 'shop_id' in merged_df.columns:
                    merged_df.drop('shop_id', axis=1, inplace=True)
            else:
                merged_df = merged_daily.copy()

            # 3. 数据预处理（数值转换与排序）
            self.signals.progress.emit("🧹 正在进行数据清洗与排序...")
            merged_df['日期_dt'] = pd.to_datetime(merged_df['日期'].astype(str), format='%Y%m%d', errors='coerce')
            merged_df.dropna(subset=['日期_dt', '商户id'], inplace=True)

            # 数值化所有计算列
            CALC_COLS = ['总订单', '毛g', '净g', '餐饮营销工具_爆单红包订单', '当日是否营业',
                         '代理商总补贴金额', '代理商付费用户加码订单量', '代理商非付费用户加码订单量',
                         '代理商付费用户加码金额', '代理商非付费用户加码金额', '净笔单30及以上订单数']
            for col in CALC_COLS:
                if col in merged_df.columns:
                    merged_df[col] = pd.to_numeric(merged_df[col], errors='coerce').fillna(0)

            # 关键：按商户和日期排序，确保动态计算逻辑正确
            merged_df = merged_df.sort_values(['商户id', '日期_dt']).reset_index(drop=True)

            # 4. 动态计算：近 30 日滑动指标 (Rolling)
            self.signals.progress.emit("⏳ 正在计算动态近 30 日指标...")
            merged_df = merged_df.set_index('日期_dt')
            rolling_map = {
                '总订单': '近30日总订单', '毛g': '近30日毛g', '净g': '近30日净g',
                '餐饮营销工具_爆单红包订单': '近30日爆单订单', '当日是否营业': '近30日营业天数',
                '净笔单30及以上订单数': '近30天净笔单30+订单数'
            }
            for src, target in rolling_map.items():
                if src in merged_df.columns:
                    merged_df[target] = merged_df.groupby('商户id')[src] \
                        .rolling(window='30D', closed='right').sum() \
                        .reset_index(level=0, drop=True)
            merged_df = merged_df.reset_index()

            # 5. 动态计算：当月累计指标 (MTD)
            self.signals.progress.emit("📅 正在计算动态当月累计指标...")
            merged_df['temp_month'] = merged_df['日期_dt'].dt.strftime('%Y%m')
            mtd_map = {
                '总订单': '当月累计总订单', '毛g': '当月累计毛g', '净g': '当月累计净g',
                '当日是否营业': '当月累计营业天数', '餐饮营销工具_爆单红包订单': '当月爆单红包订单',
                '代理商总补贴金额': '当月代理商总补贴金额', '代理商付费用户加码订单量': '当月代理商付费用户加码订单量',
                '代理商非付费用户加码订单量': '当月代理商非付费用户加码订单量',
                '代理商付费用户加码金额': '当月代理商付费用户加码金额',
                '代理商非付费用户加码金额': '当月代理商非付费用户加码金额'
            }
            for src, target in mtd_map.items():
                if src in merged_df.columns:
                    merged_df[target] = merged_df.groupby(['商户id', 'temp_month'])[src].cumsum()

            # 6. 提取全局最新月快照
            self.signals.progress.emit("🎯 正在提取最新月份快照...")
            latest_month = merged_df['temp_month'].max()

            # 每个商户只留最后一行
            final_snapshot_df = merged_df.sort_values(['商户id', '日期_dt']).drop_duplicates(subset=['商户id'],
                                                                                             keep='last').copy()

            # 过滤：仅保留最新营业日期在全局最新月内的商户
            final_snapshot_df = final_snapshot_df[final_snapshot_df['temp_month'] == latest_month]

            # 7. 整理输出与保存
            self.signals.progress.emit(f"💾 正在导出结果 (共 {len(final_snapshot_df)} 个商家)...")
            existing_columns = [col for col in self.REQUIRED_COLUMNS_ORDER if col in final_snapshot_df.columns]
            final_report_df = final_snapshot_df[existing_columns].copy()

            # 检查 output_path 是否是一个目录，如果是，自动加上文件名
            if os.path.isdir(self.output_path):
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                save_file = os.path.join(self.output_path, f"wide_report_{timestamp}.csv")
            else:
                save_file = self.output_path

            final_report_df.to_csv(save_file, index=False, encoding='utf-8-sig')

            self.signals.result.emit(final_report_df)
            self.signals.finished.emit()

        except Exception as e:
            self.signals.error.emit(f"致命错误: {type(e).__name__}: {str(e)}")
            self.signals.finished.emit()