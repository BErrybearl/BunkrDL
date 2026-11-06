"""Double-click entry point (pythonw); no console or independent scheduler."""
from bunkrdl.cli import main
from bunkrdl.launcher import notify

if __name__ == '__main__':
    try:
        raise SystemExit(main(['app']))
    except Exception as error:
        notify(f'无法启动 Bunkr 下载，请检查配置和 Python 环境。原因：{error}')
