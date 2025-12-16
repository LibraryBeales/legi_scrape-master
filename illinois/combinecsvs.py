import sys
import pandas as pd

def main():
    # Expect exactly 4 arguments: 3 input files + 1 output file
    if len(sys.argv) != 5:
        print("Usage: python combine_csvs.py file1.csv file2.csv file3.csv output.csv")
        sys.exit(1)

    file1, file2, file3, outfile = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]

    # Read the CSV files
    try:
        df1 = pd.read_csv(file1)
        df2 = pd.read_csv(file2)
        df3 = pd.read_csv(file3)
    except Exception as e:
        print(f"Error reading files: {e}")
        sys.exit(1)

    # Combine them vertically (row-wise)
    combined = pd.concat([df1, df2, df3], ignore_index=True)

    # Save the combined CSV
    try:
        combined.to_csv(outfile, index=False)
        print(f"Combined CSV written to {outfile}")
    except Exception as e:
        print(f"Error writing output file: {e}")

if __name__ == "__main__":
    main()
