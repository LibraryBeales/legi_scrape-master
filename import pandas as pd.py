import pandas as pd
import matplotlib.pyplot as plt

# === Settings ===
input_file = "illinois_phase1_fulltext_hits.csv"   
output_csv = "keyword_counts.csv"
output_png = "keyword_counts.png"
normalize_case = False        # set True for case-insensitive counting

# === Load CSV ===
df = pd.read_csv(input_file)

# Pick bill-id column (supports common variants)
bill_id_col = None
for c in ["Bill Identifier", "Policy (bill) identifier", "Bill_ID", "BillId"]:
    if c in df.columns:
        bill_id_col = c
        break
if bill_id_col is None:
    raise ValueError("Could not find a bill identifier column. Expected one of: "
                     "'Bill Identifier', 'Policy (bill) identifier', 'Bill_ID', 'BillId'.")

# === Split & normalize keywords ===
kw_series = df["Keywords"].fillna("").astype(str)
df["Keyword_List"] = kw_series.str.split(r"\s*,\s*").apply(lambda lst: [k for k in lst if k])

if normalize_case:
    df["Keyword_List"] = df["Keyword_List"].apply(lambda lst: [k.strip().lower() for k in lst])
else:
    df["Keyword_List"] = df["Keyword_List"].apply(lambda lst: [k.strip() for k in lst])

# === Total occurrences ===
all_keywords = [kw for sublist in df["Keyword_List"] for kw in sublist]
total_counts = pd.Series(all_keywords).value_counts().sort_index()

# === Alone occurrences ===
alone_mask = df["Keyword_List"].apply(lambda x: isinstance(x, list) and len(x) == 1)
alone_counts = (
    df.loc[alone_mask, "Keyword_List"]
      .apply(lambda x: x[0])
      .value_counts()
      .sort_index()
)

# === Bill Count (unique bills containing the keyword) ===
df["Keyword_Set"] = df["Keyword_List"].apply(lambda lst: sorted(set(lst)))
exploded = df[[bill_id_col, "Keyword_Set"]].explode("Keyword_Set")
bill_counts = (
    exploded.dropna(subset=["Keyword_Set"])
            .drop_duplicates(subset=[bill_id_col, "Keyword_Set"])
            .groupby("Keyword_Set")[bill_id_col]
            .nunique()
            .sort_index()
)

# === Combine results ===
keyword_counts = pd.DataFrame({
    "Total Occurrences": total_counts,
    "Alone Occurrences": alone_counts,
    "Bill Count": bill_counts
}).fillna(0).astype(int)

# Derived column for stacked bars
keyword_counts["With Others"] = (keyword_counts["Total Occurrences"] - keyword_counts["Alone Occurrences"]).clip(lower=0)

# Optional: order bars by Total (descending)
keyword_counts = keyword_counts.sort_values("Total Occurrences", ascending=False)

# === Write results ===
keyword_counts.to_csv(output_csv, index_label="Keyword")
print(f"Wrote CSV: {output_csv}")

# === Plot: Stacked bar (Alone + With Others) ===
ax = keyword_counts[["Alone Occurrences", "With Others"]].plot(
    kind="bar",
    stacked=True,
    figsize=(10, 6)
)
ax.set_xlabel("Keyword")
ax.set_ylabel("Count")
ax.set_title("Keyword Counts: Alone vs With Others")
ax.legend(title="Type")
plt.xticks(rotation=45, ha="right")
plt.tight_layout()
plt.savefig(output_png, dpi=200)
print(f"Wrote plot: {output_png}")
plt.show()
