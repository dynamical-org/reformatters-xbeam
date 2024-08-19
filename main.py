import argparse

import pandas as pd

from dynamical_reformatters import gefs_forecast

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    pipeline_args, beam_args = parser.parse_known_args()

    gefs_forecast.create_pipeline(
        pd.Timestamp("2024-01-01T00:00"), pd.Timestamp("2024-01-01T06:00"), beam_args
    )
