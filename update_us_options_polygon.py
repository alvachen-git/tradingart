from __future__ import annotations

import json
import sys

from us_options_polygon import build_arg_parser, run_cli


def main() -> None:
    parser = build_arg_parser()
    args = parser.parse_args()
    try:
        result = run_cli(args)
    except Exception as exc:
        print(json.dumps({
            "status": "error",
            "error_type": exc.__class__.__name__,
            "error": str(exc),
        }, ensure_ascii=False, indent=2, default=str))
        sys.exit(1)
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
