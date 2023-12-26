# ETL Project with AWS QuickSight Dashboard

## Overview

This repository contains the code and configurations for an Extract, Transform, Load (ETL) project with the goal of constructing a dashboard using AWS QuickSight. The project involves building the raw, trusted, and refined data layers using various tools, including Docker with Python for the raw layer, AWS Lambda for the raw layer, and AWS Glue for the trusted and refined layers.

## Project Structure

The project is organized into the following directories:

- **raw:** Contains code and configurations for the raw data layer.
- **trusted:** Contains AWS Glue scripts and configurations for the trusted data layer.
- **refined:** Contains AWS Glue scripts and configurations for the refined data layer.
- **dashboard:** Final result.

## Raw Data Layer

### Docker with Python

The raw data layer is implemented using Docker with Python. The Docker container runs Python scripts to extract data from CSV, perform basic transformations, and load the raw data into a storage solution (e.g., Amazon S3).

### AWS Lambda

Additionally, AWS Lambda functions are used for specific tasks in the raw data layer. These serverless functions can be triggered by events such as file uploads to S3 or scheduled intervals.

## Trusted and Refined Data Layers

### AWS Glue

The trusted and refined data layers are constructed using AWS Glue. AWS Glue is employed for data cataloging, ETL job orchestration, and maintaining the schema of the data.

## Trusted Layer

The trusted data layer involves cleaning, validating, and enriching the raw data. AWS Glue jobs are used to perform these transformations.

## Refined Layer

The refined data layer focuses on advanced transformations, aggregations, and preparing the data for optimal performance in the AWS QuickSight dashboard.

## AWS QuickSight Dashboard

The dashboard directory contains files related to the AWS QuickSight dashboard. This includes QuickSight manifest files, dataset definitions, and visualization configurations.

### Dashboard Objective

The primary objective of the AWS QuickSight dashboard is to analyze and showcase how technological innovations have impacted the film and series industry. The dashboard aims to provide insights into the influence of technological advancements on various aspects of the industry, offering a comprehensive view of the evolution brought about by inventions in technology.

