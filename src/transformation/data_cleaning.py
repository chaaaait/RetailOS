import pandas as pd
from datetime import datetime
from pathlib import Path


class DataCleaner:
    """
    Clean transactions data before loading into star schema.
    
    Reads from data/raw/transactions.csv (or .parquet fallback),
    applies cleaning steps, and saves cleaned data.
    """

    def __init__(self, csv_path="data/raw/transactions.csv", parquet_path="data/raw/transactions.parquet"):
        self.csv_path = Path(csv_path)
        self.parquet_path = Path(parquet_path)
        self.report = {}

    def _load_data(self) -> pd.DataFrame:
        """
        Load transactions data from CSV first, fallback to Parquet.
        Raises FileNotFoundError if neither exists.
        """
        if self.csv_path.exists():
            print(f"Reading transactions from CSV: {self.csv_path}")
            df = pd.read_csv(self.csv_path)
            return df
        elif self.parquet_path.exists():
            print(f"CSV not found. Reading transactions from Parquet: {self.parquet_path}")
            df = pd.read_parquet(self.parquet_path)
            return df
        else:
            raise FileNotFoundError(
                f"Neither transactions file found:\n"
                f"  - CSV: {self.csv_path}\n"
                f"  - Parquet: {self.parquet_path}\n"
                f"Please ensure at least one file exists."
            )

    def run(self) -> dict:
        """
        Execute all cleaning steps in order and generate report.
        
        Returns:
            dict: Cleaning report with counts for each step
        """
        print("=" * 60)
        print("Starting Data Cleaning for Transactions")
        print("=" * 60)

        # Load data
        df = self._load_data()
        
        # Print initial row count immediately
        initial_rows = len(df)
        print(f"\nInitial row count: {initial_rows}")
        self.report['initial_rows'] = initial_rows

        # STEP 1: Drop duplicate transaction_id
        print("\n[STEP 1] Dropping duplicate transaction_id...")
        before = len(df)
        df = df.drop_duplicates(subset=['transaction_id'], keep='first')
        duplicates_removed = before - len(df)
        print(f"  Removed {duplicates_removed} duplicate rows")
        self.report['duplicates_removed'] = duplicates_removed

        # STEP 2: Fill missing discount values with 0
        print("\n[STEP 2] Filling missing discount values with 0...")
        if 'discount' not in df.columns:
            print("  'discount' column not found. Creating column filled with 0.")
            df['discount'] = 0
            nulls_fixed = 0
        else:
            nulls_before = df['discount'].isna().sum()
            df['discount'] = df['discount'].fillna(0)
            nulls_fixed = nulls_before
            print(f"  Fixed {nulls_fixed} null discount values")
        self.report['nulls_fixed'] = nulls_fixed

        # STEP 3: Drop rows missing critical fields
        print("\n[STEP 3] Dropping rows missing critical fields (transaction_id, product_id, store_id)...")
        before = len(df)
        df = df.dropna(subset=['transaction_id', 'product_id', 'store_id'])
        critical_rows_removed = before - len(df)
        print(f"  Removed {critical_rows_removed} rows with missing critical fields")
        self.report['critical_rows_removed'] = critical_rows_removed

        # STEP 4: Fix negative prices
        print("\n[STEP 4] Fixing negative prices (replacing with absolute value)...")
        negative_prices = (df['price'] < 0).sum()
        df.loc[df['price'] < 0, 'price'] = df.loc[df['price'] < 0, 'price'].abs()
        print(f"  Fixed {negative_prices} negative prices")
        self.report['invalid_values_fixed'] = negative_prices

        # STEP 5: Remove zero or negative quantities
        print("\n[STEP 5] Removing zero or negative quantities...")
        before = len(df)
        df = df[df['quantity'] > 0]
        invalid_quantity_removed = before - len(df)
        print(f"  Removed {invalid_quantity_removed} rows with invalid quantities")
        self.report['invalid_quantity_removed'] = invalid_quantity_removed

        # STEP 6: Remove future timestamps
        print("\n[STEP 6] Removing future timestamps...")
        before = len(df)
        # Handle date column (could be string or datetime)
        if df['date'].dtype == 'object':
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
        
        current_time = datetime.now()
        df = df[df['date'] <= current_time]
        future_rows_removed = before - len(df)
        print(f"  Removed {future_rows_removed} rows with future timestamps")
        self.report['future_rows_removed'] = future_rows_removed

        # STEP 7: Flag statistical anomalies (DO NOT REMOVE)
        print("\n[STEP 7] Flagging statistical anomalies (price > mean + 3*std)...")
        mean_price = df['price'].mean()
        std_price = df['price'].std()
        threshold = mean_price + 3 * std_price
        df['is_anomaly'] = df['price'] > threshold
        anomalies_flagged = df['is_anomaly'].sum()
        print(f"  Flagged {anomalies_flagged} anomalies (threshold: {threshold:.2f})")
        print(f"  Mean price: {mean_price:.2f}, Std: {std_price:.2f}")
        self.report['anomalies_flagged'] = int(anomalies_flagged)

        # Final row count
        final_rows = len(df)
        print(f"\nFinal row count: {final_rows}")
        self.report['final_rows'] = final_rows

        # Save cleaned dataset
        output_path = Path("data/raw/transactions_cleaned.parquet")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(output_path, index=False)
        print(f"\nCleaned data saved to: {output_path}")

        # Generate and save report
        self._save_report()

        print("\n" + "=" * 60)
        print("Data Cleaning Complete!")
        print("=" * 60)

        return self.report

    def _save_report(self):
        """Save cleaning report to docs/DATA_QUALITY.md"""
        docs_dir = Path("docs")
        docs_dir.mkdir(parents=True, exist_ok=True)
        
        report_path = docs_dir / "DATA_QUALITY.md"
        
        with open(report_path, "w", encoding="utf-8") as f:
            f.write("# Data Quality Report\n\n")
            f.write(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            f.write("---\n\n")
            
            f.write("## Summary\n\n")
            f.write(f"- **Initial Rows:** {self.report['initial_rows']:,}\n")
            f.write(f"- **Final Rows:** {self.report['final_rows']:,}\n")
            f.write(f"- **Rows Removed:** {self.report['initial_rows'] - self.report['final_rows']:,}\n")
            f.write(f"- **Retention Rate:** {(self.report['final_rows'] / self.report['initial_rows'] * 100):.2f}%\n\n")
            
            f.write("---\n\n")
            f.write("## Cleaning Steps\n\n")
            
            f.write(f"### Step 1: Duplicate Removal\n")
            f.write(f"- **Duplicates Removed:** {self.report['duplicates_removed']:,}\n\n")
            
            f.write(f"### Step 2: Missing Value Handling\n")
            f.write(f"- **Null Discounts Fixed:** {self.report['nulls_fixed']:,}\n\n")
            
            f.write(f"### Step 3: Critical Field Validation\n")
            f.write(f"- **Rows Removed (Missing Critical Fields):** {self.report['critical_rows_removed']:,}\n\n")
            
            f.write(f"### Step 4: Data Correction\n")
            f.write(f"- **Negative Prices Fixed:** {self.report['invalid_values_fixed']:,}\n\n")
            
            f.write(f"### Step 5: Quantity Validation\n")
            f.write(f"- **Invalid Quantities Removed:** {self.report['invalid_quantity_removed']:,}\n\n")
            
            f.write(f"### Step 6: Temporal Validation\n")
            f.write(f"- **Future Timestamps Removed:** {self.report['future_rows_removed']:,}\n\n")
            
            f.write(f"### Step 7: Anomaly Detection\n")
            f.write(f"- **Anomalies Flagged:** {self.report['anomalies_flagged']:,}\n")
            f.write(f"- **Note:** Anomalies are flagged but NOT removed.\n\n")
            
            f.write("---\n\n")
            f.write("## Detailed Statistics\n\n")
            f.write("```\n")
            for key, value in self.report.items():
                f.write(f"{key}: {value:,}\n")
            f.write("```\n")
        
        print(f"Report saved to: {report_path}")


if __name__ == "__main__":
    cleaner = DataCleaner()
    cleaner.run()
