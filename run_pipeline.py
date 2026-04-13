from shield_pipeline.config import PipelineConfig
from shield_pipeline.pipeline import run_pipeline


def main() -> None:
    cfg = PipelineConfig()
    run_pipeline(cfg)
    print("Pipeline complete. Outputs saved in data/processed/")


if __name__ == "__main__":
    main()

