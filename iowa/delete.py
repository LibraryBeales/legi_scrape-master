import pandas as pd

# Input and output file paths
input_file = "iowa_bills_keywords_dates.csv"          # change this to your file
output_file = "iowa_bills_keywords_dates2.csv"  # cleaned file

# Read the CSV
df = pd.read_csv(input_file)

# Drop the unwanted columns (ignore if not present)
df = df.drop(columns=["Policy sponsor","Policy sponsor party","Link to bill","bill text,Cosponsor"], errors="ignore")

# Save the cleaned CSV
df.to_csv(output_file, index=False)

print(f"Cleaned CSV saved as {output_file}")
