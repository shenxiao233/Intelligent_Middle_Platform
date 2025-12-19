# worker.py

from PySide6.QtCore import QThread, Signal
import data_processor  # 引入业务逻辑模块


# --- Worker 线程：用于运行耗时的数据处理逻辑，防止界面卡顿 ---
class Worker(QThread):
    # 定义信号，作为“后端 API”的返回接口
    finished = Signal(str)  # 成功：返回输出文件路径
    error = Signal(str)  # 失败：返回错误信息

    def __init__(self, f1, f2, output_dir):
        super().__init__()
        self.f1 = f1
        self.f2 = f2
        self.output_dir = output_dir

    def run(self):
        try:
            # 调用纯粹的业务逻辑函数（Model 层）
            output_path = data_processor.process_data_logic(self.f1, self.f2, self.output_dir)

            # 通过信号 (API) 发送成功结果给主线程
            self.finished.emit(output_path)

        except Exception as e:
            # 通过信号 (API) 发送错误信息给主线程
            self.error.emit(str(e))