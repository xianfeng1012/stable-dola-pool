# dola-pool

dola.com（字节 Seedance 视频模型）号池 → OpenAI 兼容视频 API 分发服务。

## 结构

| 文件 | 作用 |
|---|---|
| `dola_client.py` | 上游 SDK（协议逆向，来自 astrbot_plugin_doubao_free） |
| `pool.py` | 号池管理（cookie 加载 + 多账号轮询 + 并发信号量） |
| `store.py` | 任务状态持久化（SQLite） |
| `server.py` | FastAPI 服务，OpenAI 兼容接口 |
| `config.py` | 配置（环境变量） |
| `cookies.txt` | 号池 cookie（一行一个，gitignore） |

## 部署

```bash
# 1. 安装依赖（用 venv 隔离）
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt

# 2. 准备号池
cp cookies.txt.example cookies.txt
# 编辑 cookies.txt，填入真实 cookie（一行一个）

# 3. 配置代理（dola.com 需 JP/KR 出口，服务器在大陆必须配）
export HTTPS_PROXY=http://user:pass@jp-proxy:port   # Windows PowerShell: $env:HTTPS_PROXY=...
export HTTP_PROXY=$HTTPS_PROXY

# 4. 配置服务
export DOLA_API_KEYS=sk-xxx        # 对外 API key，逗号分隔多个
export DOLA_MAX_CONCURRENCY=3      # 并发任务上限
export DOLA_VIDEO_TIMEOUT=300      # 出片超时（秒）

# 5. 启动
uvicorn server:app --host 0.0.0.0 --port 8000
```

## API 用法

对外固定视频时长为 **10 秒、15 秒、30 秒**。创建视频接口默认使用 10 秒；每个数据库 API Key 还可以在管理面板中单独设置每日额度、并发上限和允许时长。

```bash
# 创建视频任务
curl -X POST http://127.0.0.1:8000/v1/videos/generations \
  -H "Authorization: Bearer sk-xxx" \
  -H "Content-Type: application/json" \
  -d '{"model":"seedance-v2.0","prompt":"一只猫追蝴蝶","size":"720x1280","duration":15}'
# -> {"id":"video_xxx","status":"queued","model":"seedance-v2.0","prompt":"..."}

# 查询任务（status: queued -> processing -> completed / failed）
curl http://127.0.0.1:8000/v1/videos/video_xxx \
  -H "Authorization: Bearer sk-xxx"
# -> {"id":"video_xxx","status":"completed","video_url":"https://..."}

# 健康检查（号池可用账号数）
curl http://127.0.0.1:8000/health
```

## 说明

- 硬限制 2 片 / 号 / 天，号池越大吞吐越高。
- API Key 支持每日任务上限、同时生成上限、允许时长和过期时间；配置为 0 表示不限。
- 任务会记录 API Key 名称、客户级用量和实际使用账号；服务端待处理任务上限由 `DOLA_MAX_PENDING_TASKS` 控制，默认 100。
- `video_url` 是 dola CDN 临时链接（约数小时过期），正式分发需在 `_run_task` 里下载转存到自己的 OSS/对象存储。
- cookie 有效期约 60 天，过期需重新登录更新 `cookies.txt`。
- 国内版 doubao.com 对 Python HTTP 客户端有风控，本服务只跑国际版 dola.com。

## 依赖的 Dola 扩展（未随仓库分发）

`30 秒时长` 与 `无水印解析` 依赖一个第三方 Chromium 扩展（unpacked），放在 `extensions/dola30/`。
该扩展是别人出售的付费资源，不包含在本仓库中，需要自备。缺少它时：

- `duration=30` 会直接报错（`browser.py` 抛 `Dola 扩展目录不存在`）；
- 设 `DOLA_EXTENSION_ENABLED=0` 可关闭扩展路径，只跑网页端原生支持的 5/10 秒。

## 免责声明

本仓库是个人学习与互操作性研究记录，包含对上游网页接口的逆向实现；代码不含任何账号、cookie、密钥或可用凭据。使用者自行评估账号与合规风险。
