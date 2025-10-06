#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Merge Iowa bill CSVs (schema #1 + schema #2).

CSV #1 columns:
  State,GA,Policy (bill) identifier,Policy sponsor,Policy sponsor party,
  Link to bill,bill text,Cosponsor,Act identifier,Matched keywords

CSV #2 columns:
  State,GA,Policy (bill) identifier,Introduced date,Effective date,
  Passed introduced chamber date,Passed second chamber date,
  Dead date,Enacted (Y/N),Enacted Date,Matched keywords

Join keys: State + GA + Policy (bill) identifier
Coalesce: Matched keywords (prefer CSV #1 value)
"""

import argparse
import pandas as pd
from collections import OrderedDict

JOIN_KEYS = ["State", "GA", "Policy (bill) identifier"]
OVERLAP = ["Matched keywords"]

CSV1_ORDER = [
    "State", "GA", "Policy (bill) identifier",
    "Policy sponsor", "Policy sponsor party",
    "Link to bill", "bill text", "Cosponsor",
    "Act identifier", "Matched keywords"
]

CSV2_ORDER = [
    "Introduced date", "Effective date",
    "Passed introduced chamber date",
    "Passed second chamber date",
    "Dead date", "Enacted (Y/N)", "Enacted Date"
]

def coalesce_columns(df, col):
    cx, cy = f"{col}_x", f"{col}_y"
    if cx in df.columns and cy in df.columns:
        df[col] = df[cx].where(df[cx].notna(), df[cy])
        df.drop(columns=[cx, cy], inplace=True)
    elif cx in df.columns:
        df.rename(columns={cx: col}, inplace=True)
    elif cy in df.columns:
        df.rename(columns={cy: col}, inplace=True)

def main():
    ap = argparse.ArgumentParser(description="Merge two Iowa bill CSVs")
    ap.add_argument("csv1", help="CSV #1 path")
    ap.add_argument("csv2", help="CSV #2 path")
    ap.add_argument("-o", "--output", default="merged_bills.csv", help="Output file")
    ap.add_argument("--how", choices=["inner", "left", "right", "outer"], default="inner")
    args = ap.parse_args()

    df1 = pd.read_csv(args.csv1, dtype=str)
    df2 = pd.read_csv(args.csv2, dtype=str)

    # Clean whitespace-only cells
    df1 = df1.applymap(lambda x: None if isinstance(x, str) and x.strip() == "" else x)
    df2 = df2.applymap(lambda x: None if isinstance(x, str) and x.strip() == "" else x)

    merged = pd.merge(df1, df2, on=JOIN_KEYS, how=args.how, suffixes=("_x", "_y"))

    for col in OVERLAP:
        coalesce_columns(merged, col)

    # Final order
    final_cols = []
    for c in CSV1_ORDER:
        if c in merged.columns:
            final_cols.append(c)
    for c in CSV2_ORDER:
        if c in merged.columns and c not in final_cols:
            final_cols.append(c)
    # Any leftovers
    leftovers = [c for c in merged.columns if c not in final_cols and not c.endswith(("_x", "_y"))]
    final_cols = list(OrderedDict.fromkeys(final_cols + leftovers))

    merged = merged[final_cols]
    merged.to_csv(args.output, index=False)

    print(f"Merged CSV saved as {args.output}")
    print(f"Rows: {len(merged)} | Cols: {len(merged.columns)}")

if __name__ == "__main__":
    main()
