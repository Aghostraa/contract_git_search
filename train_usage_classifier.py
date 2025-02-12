#!/usr/bin/env python3

import os
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.pipeline import Pipeline
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report, accuracy_score
import joblib


def main():
    # ------------------------
    # 1. Configuration
    # ------------------------
    CSV_PATH = "contract_references.csv"  # Your CSV file
    MODEL_OUT = "usage_classifier.joblib"  # Where to save the model

    # ------------------------
    # 2. Load data
    # ------------------------
    if not os.path.isfile(CSV_PATH):
        print(f"Error: {CSV_PATH} not found.")
        return

    df = pd.read_csv(CSV_PATH)
    print(f"Loaded {len(df)} rows from {CSV_PATH}.")

    # ------------------------
    # 3. Filter out rows with no usage_category
    # ------------------------
    if "usage_category" not in df.columns:
        print("Error: usage_category column not found in CSV.")
        return

    # Drop rows where usage_category is NaN or blank
    df = df.dropna(subset=["usage_category"])
    df = df[df["usage_category"].str.strip() != ""]

    if df.empty:
        print("No valid rows with usage_category. Exiting.")
        return

    # ------------------------
    # 4. Combine text fields
    # ------------------------
    # We'll create a new column that merges snippet_text, repo_description, file_path, etc.
    def combine_text_fields(row):
        snippet = str(row.get("snippet_text", ""))
        description = str(row.get("repo_description", ""))
        file_path = str(row.get("file_path", ""))
        repo_full_name = str(row.get("repo_full_name", ""))
        # Combine them into one string
        return " ".join([snippet, description, file_path, repo_full_name])

    df["combined_text"] = df.apply(combine_text_fields, axis=1)

    # Features (X) and label (y)
    X = df["combined_text"]
    y = df["usage_category"]

    # ------------------------
    # 5. Split train/test
    # ------------------------
    # Let's do an 80/20 split; random_state ensures reproducibility
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    print(f"Training set size: {len(X_train)}")
    print(f"Test set size:     {len(X_test)}")

    # ------------------------
    # 6. Build a pipeline
    # ------------------------
    # We'll use TfidfVectorizer for text, then a simple Logistic Regression.
    pipeline = Pipeline([
        ("tfidf", TfidfVectorizer(
            # You can tweak these parameters:
            # ngram_range=(1,2),
            # max_features=5000,
            # stop_words="english",
        )),
        ("clf", LogisticRegression(
            max_iter=200,  # if you see convergence warnings, increase this
            random_state=42
        ))
    ])

    # ------------------------
    # 7. Train (fit) the model
    # ------------------------
    pipeline.fit(X_train, y_train)
    print("\nModel training complete.")

    # ------------------------
    # 8. Evaluate
    # ------------------------
    y_pred = pipeline.predict(X_test)

    print("\nTest Set Evaluation:")
    print(f"Accuracy: {accuracy_score(y_test, y_pred):.3f}\n")
    print("Classification Report:")
    print(classification_report(y_test, y_pred))

    # ------------------------
    # 9. Save the model
    # ------------------------
    joblib.dump(pipeline, MODEL_OUT)
    print(f"Model pipeline saved to {MODEL_OUT}")


if __name__ == "__main__":
    main()
