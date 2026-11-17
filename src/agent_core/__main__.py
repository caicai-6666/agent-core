"""离线文档入口，不创建模型客户端或读取业务配置。"""
from importlib.resources import files
from pathlib import Path
import argparse


def main():
    parser = argparse.ArgumentParser(description='Agent Core 随包开发文档')
    parser.add_argument('command', choices=['docs'])
    parser.parse_args()
    packaged = Path(str(files('agent_core').joinpath('docs/README.md')))
    source = Path(__file__).resolve().parents[2]/'README.md'
    path = packaged if packaged.exists() else source
    if not path.exists():
        parser.error('未找到文档，请重新安装完整发行包')
    print(path)
    print(path.read_text(encoding='utf-8'))


if __name__ == '__main__':
    main()
