from typing import Dict, Iterable, Iterator, Optional

import csv
import io
import zipfile

import requests
import dlt


DEFAULT_GTFS_STATIC_ZIP = (
    # Public, supplemented GTFS static bundle per MTA
    # https://rrgtfsfeeds.s3.amazonaws.com/gtfs_supplemented.zip
    "https://rrgtfsfeeds.s3.amazonaws.com/gtfs_supplemented.zip"
)


def _download_zip(url: str) -> zipfile.ZipFile:
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    return zipfile.ZipFile(io.BytesIO(resp.content))


def _iter_csv(z: zipfile.ZipFile, member: str) -> Iterator[Dict]:
    with z.open(member, "r") as fh:
        text = io.TextIOWrapper(fh, encoding="utf-8")
        reader = csv.DictReader(text)
        for row in reader:
            yield {k: (v if v != "" else None) for k, v in row.items()}


@dlt.source(name="mta_gtfs_static")
def mta_static_source(
    zip_url: Optional[str] = dlt.config.value,  # override via config
) -> Iterable:
    url = zip_url or DEFAULT_GTFS_STATIC_ZIP
    zf = _download_zip(url)

    @dlt.resource(name="routes", primary_key="route_id", write_disposition="merge")
    def routes() -> Iterator[Dict]:
        yield from _iter_csv(zf, "routes.txt")

    @dlt.resource(name="stops", primary_key="stop_id", write_disposition="merge")
    def stops() -> Iterator[Dict]:
        yield from _iter_csv(zf, "stops.txt")

    @dlt.resource(name="trips", primary_key="trip_id", write_disposition="merge")
    def trips() -> Iterator[Dict]:
        yield from _iter_csv(zf, "trips.txt")

    @dlt.resource(name="stop_times", write_disposition="append")
    def stop_times() -> Iterator[Dict]:
        yield from _iter_csv(zf, "stop_times.txt")

    @dlt.resource(name="calendar", primary_key="service_id", write_disposition="merge")
    def calendar() -> Iterator[Dict]:
        yield from _iter_csv(zf, "calendar.txt")

    return [routes, stops, trips, stop_times, calendar]


