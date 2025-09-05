import argparse
import base64
import datetime
import json
import os
import sys
from typing import Dict, Optional

import requests
from google.protobuf.json_format import MessageToDict
from google.transit import gtfs_realtime_pb2


def get_feed_urls() -> Dict[str, str]:
    """
    Return the known MTA Subway GTFS-realtime feed URLs.

    Duplicated here so this script can run standalone without importing
    package modules. Keep in sync with `mta_gtfs_realtime.py`.
    """
    return {
        "ace": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-ace",
        "bdfm": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-bdfm",
        "g": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-g",
        "jz": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-jz",
        "l": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-l",
        "nqrw": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-nqrw",
        "main": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs",
        "sir": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-si",
        # Alerts feed (GTFS-rt Alerts) requested
        "alerts": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/camsys%2Fsubway-alerts",
    }


def fetch_raw_payload(url: str, api_key: Optional[str], timeout: int = 30) -> bytes:
    headers = {"x-api-key": api_key} if api_key else None
    response = requests.get(url, headers=headers, timeout=timeout)
    response.raise_for_status()
    return response.content


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Download raw GTFS-realtime protobuf payload(s) from MTA and write to files. "
            "If provided, the API key is taken from --api-key or $MTA_API_KEY."
        )
    )
    parser.add_argument(
        "--feed",
        choices=list(get_feed_urls().keys()) + ["all"],
        default="main",
        help="Which feed to fetch (default: main). Use 'all' to fetch every feed.",
    )
    parser.add_argument(
        "--api-key",
        default=os.environ.get("MTA_API_KEY", ""),
        help="Optional MTA API key. If omitted, request is sent without x-api-key.",
    )
    parser.add_argument(
        "--out-dir",
        default=os.path.join(os.getcwd(), "raw_payloads"),
        help="Directory to write .pb files to (will be created if missing).",
    )
    parser.add_argument(
        "--stdout",
        action="store_true",
        help=(
            "Also print base64 of the full raw payload to stdout (CAUTION: large). "
            "If multiple feeds are selected, prints each sequentially."
        ),
    )
    parser.add_argument(
        "--write-json",
        action="store_true",
        help="Also write a JSON conversion of the protobuf next to the .pb file.",
    )
    parser.add_argument(
        "--only-json",
        action="store_true",
        help="Write only JSON (skip writing the raw .pb file). Implies --write-json.",
    )
    parser.add_argument(
        "--pretty",
        action="store_true",
        help="Pretty-print JSON with indentation (larger files).",
    )
    args = parser.parse_args()

    feed_urls = get_feed_urls()
    selected = (
        list(feed_urls.items()) if args.feed == "all" else [(args.feed, feed_urls[args.feed])]
    )

    os.makedirs(args.out_dir, exist_ok=True)

    for feed_name, url in selected:
        timestamp = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        print(f"Fetching feed '{feed_name}' from {url} ...")
        data = fetch_raw_payload(url, args.api_key)
        wrote_raw = False
        if not args.only_json:
            out_path = os.path.join(args.out_dir, f"gtfs_raw_{feed_name}_{timestamp}.pb")
            with open(out_path, "wb") as f:
                f.write(data)
            wrote_raw = True

            preview_hex = data[:64].hex()
            print(
                f"Wrote {len(data)} bytes to {out_path}\n"
                f"First 64 bytes (hex): {preview_hex}"
            )

        if args.stdout:
            print("\nBase64 (full payload):")
            print(base64.b64encode(data).decode("ascii"))
            print()

        if args.write_json or args.only_json:
            # Parse protobuf and convert to dict
            feed_msg = gtfs_realtime_pb2.FeedMessage()
            feed_msg.ParseFromString(data)
            feed_dict = MessageToDict(feed_msg, preserving_proto_field_name=True)

            json_path = os.path.join(args.out_dir, f"gtfs_{feed_name}_{timestamp}.json")
            with open(json_path, "w", encoding="utf-8") as jf:
                if args.pretty:
                    json.dump(feed_dict, jf, ensure_ascii=False, indent=2)
                else:
                    json.dump(feed_dict, jf, ensure_ascii=False, separators=(",", ":"))

            size_bytes = os.path.getsize(json_path)
            wrote = " and .pb" if wrote_raw else ""
            print(f"Wrote JSON ({size_bytes} bytes) to {json_path}{wrote}")


if __name__ == "__main__":
    main()


