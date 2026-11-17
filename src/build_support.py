"""将唯一来源的接入文档随 wheel 分发，避免 pip 用户拿不到适配契约。"""
from pathlib import Path
import shutil
from setuptools.command.build_py import build_py


class BuildPy(build_py):
    def run(self):
        super().run()
        root = Path(__file__).resolve().parents[1]
        target = Path(self.build_lib)/'agent_core/docs'
        target.mkdir(parents=True, exist_ok=True)
        shutil.copytree(root/'description', target/'description', dirs_exist_ok=True)
        for name in ('README.md', 'AGENTS.md'):
            shutil.copy2(root/name, target/name)
        shutil.copytree(root/'examples', target/'examples', dirs_exist_ok=True,
            ignore=shutil.ignore_patterns('__pycache__'))
