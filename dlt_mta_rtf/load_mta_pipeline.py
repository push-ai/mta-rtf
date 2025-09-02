from typing import Optional

import dlt

from mta_gtfs_realtime import mta_rt_source
from mta_gtfs_static import mta_static_source


def run(full_static: bool = False, feeds: Optional[list[str]] = None) -> None:
    pipeline = dlt.pipeline(
        pipeline_name="mta_subway",
        destination="bigquery",
        dataset_name="mta_subway",
    )

    # Load static GTFS reference tables for names/labels
    static_src = mta_static_source()
    if full_static:
        pipeline.run(static_src)
    else:
        # Load only dimension tables by default
        pipeline.run([static_src.routes, static_src.stops, static_src.trips])

    # Load GTFS-realtime entities
    rt_src = mta_rt_source(feeds=feeds)
    pipeline.run(rt_src)


if __name__ == "__main__":
    run()


