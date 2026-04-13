import argparse
import os

from shield_pipeline.weather.producer import run_producer_loop


def main() -> None:
    parser = argparse.ArgumentParser(description="Publish airport weather observations to Kafka.")
    parser.add_argument(
        "--interval",
        type=float,
        default=float(os.environ.get("WEATHER_INTERVAL_SEC", "300")),
        help="Seconds between full airport sweep (default 300).",
    )
    parser.add_argument("--once", action="store_true", help="Fetch and publish one sweep then exit.")
    args = parser.parse_args()
    run_producer_loop(interval_sec=args.interval, once=args.once)


if __name__ == "__main__":
    main()
