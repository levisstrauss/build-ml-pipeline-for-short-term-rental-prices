#!/usr/bin/env python
"""
Download from W&B the raw dataset and apply some basic data cleaning, exporting the result to a new artifact
"""
import argparse
import logging
import wandb
import pandas as pd


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):

    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    # Download input artifact. This will also log that this script is using this
    # particular version of the artifact
    # artifact_local_path = run.use_artifact(args.input_artifact).file()

    ######################
    # YOUR CODE HERE     #
    ######################

    logger.info(f"Fetching {args.input_artifact} from W&B...")
    artifact_local_path = run.use_artifact(args.input_artifact).file()

    logger.info("Reading the data with pandas...")

    df = pd.read_csv(artifact_local_path)

    # Dropping outliers
    min_price = float(args.min_price)
    max_price = float(args.max_price)
    idx = df['price'].between(min_price, max_price)
    df = df[idx]

    # Converting last review to datetime
    df['last_review'] = pd.to_datetime(df['last_review'])

    # Replace Empty date with old one
    df['last_review'].fillna(pd.to_datetime("2010-01-01"), inplace=True)


    # Replace all empty review by 0
    df['reviews_per_month'].fillna(0, inplace=True)

    # Fill empty name something short
    df['name'].fillna('-', inplace=True)
    df['host_name'].fillna('-', inplace=True)

     # Filter some datapoints here
    idx = df['longitude'].between(-74.25, -73.50) & df['latitude'].between(40.5, 41.2)
    df = df[idx].copy()

     # Save the clean data after preprocessing
    df.to_csv("clean_sample.csv", index=False)


    # Create an artifact
    artifact = wandb.Artifact(
        args.output_name,
        type=args.output_type,
        description=args.output_description,
    )

    # Add the data to the artifact
    artifact.add_file("clean_sample.csv")

    # Add the artifact to the run
    run.log_artifact(artifact)

    # Commit everything
    run.finish()


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="A very basic data cleaning")


    parser.add_argument(
        "--input_artifact", 
        type=str,
        help="Input Artifact",
        required=True
    )

    parser.add_argument(
        "--output_artifact_name",
        type=str,
        help="Output Artifact Name",
        required=True
    )

    parser.add_argument(
        "--output_type", 
        type=str,
        help="Output File Type",
        required=True
    )

    parser.add_argument(
        "--output_description", 
        type=str,
        help="Output File Description",
        required=True
    )

    parser.add_argument(
        "--min_price", 
        type=float,
        help="Minimum Price",
        required=True
    )

    parser.add_argument(
        "--max_price", 
        type=float,
        help="Maximum Price",
        required=True
    )


    args = parser.parse_args()

    go(args)
