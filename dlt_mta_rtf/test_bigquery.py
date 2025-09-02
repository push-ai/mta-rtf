import datetime
import dlt


def main() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="mta_rtf_bq_test",
        destination="bigquery",
        dataset_name="mta_rtf_test",
    )

    data = [
        {
            "id": 1,
            "message": "bigquery connectivity ok",
            "loaded_at": datetime.datetime.utcnow().isoformat(),
        }
    ]

    load_info = pipeline.run(data, table_name="ping")
    print(load_info)


if __name__ == "__main__":
    main()


