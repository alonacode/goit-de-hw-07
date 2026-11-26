#!/usr/bin/env bash
# Завантажує публічний олімпійський датасет (Olympedia, Kaggle) у data/
set -euo pipefail
cd "$(dirname "$0")/.."

URL="https://www.kaggle.com/api/v1/datasets/download/josephcheng123456/olympic-historical-dataset-from-olympediaorg"
FILE="Olympic_Athlete_Event_Results.csv"

mkdir -p data
curl -fSL -o data/dataset.zip "$URL"
unzip -o data/dataset.zip "$FILE" -d data
rm data/dataset.zip
