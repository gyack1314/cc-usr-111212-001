"""显示原始湖仓资料配置，或按快照还原可见行。"""
import argparse
import json
from pathlib import Path

from .io import load
from .domain import summarize
from .reader import read_snapshot


def _dumps(payload):
    return json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False,
                      sort_keys=False)


def main():
    p = argparse.ArgumentParser(prog='lake_data.cli')
    sub = p.add_subparsers(dest='command', required=True)

    p_inspect = sub.add_parser('inspect', help='只显示原字段概况，不应用删除')
    p_inspect.add_argument('file')

    p_read = sub.add_parser('read', help='按快照清单还原可见行与删除证据')
    p_read.add_argument('file')

    a = p.parse_args()
    if a.command == 'inspect':
        v, digest = load(a.file)
        payload = {
            'source_sha256': digest,
            'summary': summarize(v, Path(a.file).parent),
        }
    else:
        payload = read_snapshot(a.file)
    print(_dumps(payload))


if __name__ == '__main__':
    main()
