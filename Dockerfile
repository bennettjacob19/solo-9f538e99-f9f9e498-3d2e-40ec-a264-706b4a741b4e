FROM python:3.11-slim

# tini 负责回收被 reparent 的孤儿/僵尸进程（步骤进程组清理的最后一环）
RUN apt-get update \
    && apt-get install -y --no-install-recommends tini \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY examples ./examples

# 数据根目录：SQLite、工作目录、日志，compose 会在此挂载命名卷
ENV PIPELINE_DATA_DIR=/data
RUN mkdir -p /data

EXPOSE 8000

# 单 worker 才能保证全局两步并行信号量与内存调度状态一致
ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
