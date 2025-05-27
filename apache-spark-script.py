from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract, lit, udf
from pyspark.sql.types import StringType
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import pandas as pd
import json
import re

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("SmartSwiftLogAnalyzer") \
    .config("spark.hadoop.fs.s3a.access.key", "<s3-access-key>") \
    .config("spark.hadoop.fs.s3a.secret.key", "<s3-secret-key>") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# Generate date range (last 5 days including today)
def generate_date_range(days_back=1):
    dates = []
    for i in range(days_back + 1):
        date = (datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d')
        dates.append(date)
    return dates

# Function to extract data from JSON within logs
def extract_transaction_data(log_text):
    try:
        # Extract the JSON part from api_content
        api_match = re.search(r'api_content:\s*({.*})', log_text)
        if api_match:
            json_str = api_match.group(1)
            data = json.loads(json_str)
            
            # Extract relevant fields
            result = {
                'api_status': str(data.get("status", "")),
                'api_message': data.get("message", ""),
                'transaction_status': data.get("data", {}).get("status", ""),
                'send_currency': data.get("data", {}).get("sender", {}).get("send_currency", ""),
                'receive_currency': data.get("data", {}).get("quotation", {}).get("receive_currency", ""),
                'receive_amount': str(data.get("data", {}).get("quotation", {}).get("receive_amount", "")),
                'send_amount': str(data.get("data", {}).get("quotation", {}).get("deposit_amount", "")),
                'exchange_rate': str(data.get("data", {}).get("quotation", {}).get("rates", "")),
                'sender_country': data.get("data", {}).get("sender", {}).get("address", {}).get("country", ""),
                'receiver_country': data.get("data", {}).get("receiver", {}).get("address", {}).get("country", ""),
                'sender_name': f"{data.get('data', {}).get('sender', {}).get('first_name', '')} {data.get('data', {}).get('sender', {}).get('last_name', '')}".strip(),
                'receiver_name': f"{data.get('data', {}).get('receiver', {}).get('first_name', '')} {data.get('data', {}).get('receiver', {}).get('last_name', '')}".strip()
            }
            return json.dumps(result)
    except Exception as e:
        pass
    return json.dumps({})

# Register UDF
extract_data_udf = udf(extract_transaction_data, StringType())

# Get dates to process
date_list = generate_date_range(1)
print(f"Processing SmartSwift logs for dates: {date_list}")

all_data = []
summary_stats = {}

for date_str in date_list:
    s3_path = f"s3a://fintech-logs-prod/iPay/PayoutMiddleware/SmartSwift/{date_str}/*.txt"
    print(f"\n{'='*80}")
    print(f"PROCESSING LOGS FOR {date_str}")
    print(f"{'='*80}")
    
    try:
        # Read logs
        df = spark.read.text(s3_path)
        record_count = df.count()
        
        if record_count > 0:
            print(f"Found {record_count} log entries")
            
            # Parse the basic log structure
            df_parsed = df.select(
                col("value").alias("raw_log"),
                regexp_extract(col("value"), r"\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\]", 1).alias("timestamp"),
                regexp_extract(col("value"), r"Partner: ([^,]+)", 1).alias("partner"),
                regexp_extract(col("value"), r"Status: ([^,]+)", 1).alias("log_status"),
                regexp_extract(col("value"), r"Category: ([^,]+)", 1).alias("category"),
                regexp_extract(col("value"), r"State: ([^,]+)", 1).alias("state"),
                regexp_extract(col("value"), r"ControlNo: ([^,]+)", 1).alias("control_no"),
                extract_data_udf(col("value")).alias("transaction_data"),
                lit(date_str).alias("log_date")
            ).filter(col("timestamp") != "")
            
            print(f"\nSAMPLE PARSED LOGS:")
            print("-" * 80)
            df_parsed.select("timestamp", "partner", "category", "state", "control_no").show(10, truncate=False)
            
            # Show detailed transaction info for first few records
            sample_data = df_parsed.limit(3).collect()
            
            for i, row in enumerate(sample_data, 1):
                print(f"\n--- TRANSACTION {i} DETAILS ---")
                print(f"Timestamp: {row['timestamp']}")
                print(f"Partner: {row['partner']}")
                print(f"Category: {row['category']}")
                print(f"State: {row['state']}")
                print(f"Control No: {row['control_no']}")
                
                # Parse transaction data
                try:
                    trans_data = json.loads(row['transaction_data'])
                    if trans_data:
                        print(f"Transaction Status: {trans_data.get('transaction_status', 'N/A')}")
                        print(f"Currency: {trans_data.get('send_currency', 'N/A')} → {trans_data.get('receive_currency', 'N/A')}")
                        print(f"Amount: {trans_data.get('send_amount', 'N/A')} → {trans_data.get('receive_amount', 'N/A')}")
                        print(f"Exchange Rate: {trans_data.get('exchange_rate', 'N/A')}")
                        print(f"Corridor: {trans_data.get('sender_country', 'N/A')} → {trans_data.get('receiver_country', 'N/A')}")
                        print(f"Sender: {trans_data.get('sender_name', 'N/A')}")
                        print(f"Receiver: {trans_data.get('receiver_name', 'N/A')}")
                except:
                    print("Could not parse transaction details")
                print("-" * 50)
            
            # Calculate statistics
            state_stats = df_parsed.groupBy("state").count().collect()
            category_stats = df_parsed.groupBy("category").count().collect()
            
            summary_stats[date_str] = {
                'total_records': record_count,
                'states': {row['state']: row['count'] for row in state_stats},
                'categories': {row['category']: row['count'] for row in category_stats}
            }
            
            print(f"\nSTATISTICS FOR {date_str}:")
            print(f"Total Records: {record_count}")
            print("States Distribution:")
            for state, count in summary_stats[date_str]['states'].items():
                print(f"  {state}: {count}")
            print("Categories Distribution:")
            for category, count in summary_stats[date_str]['categories'].items():
                print(f"  {category}: {count}")
            
            # Convert to pandas for analysis
            pandas_df = df_parsed.toPandas()
            pandas_df['date'] = date_str
            all_data.append(pandas_df)
            
        else:
            print(f"No logs found for {date_str}")
            summary_stats[date_str] = {'total_records': 0, 'states': {}, 'categories': {}}
            
    except Exception as e:
        print(f"Error processing {date_str}: {str(e)}")
        summary_stats[date_str] = {'total_records': 0, 'states': {}, 'categories': {}}

# Create visualizations if we have data
if all_data:
    print(f"\n{'='*80}")
    print("CREATING VISUALIZATIONS")
    print(f"{'='*80}")
    
    # Combine all data
    combined_df = pd.concat(all_data, ignore_index=True)
    
    # Parse transaction data for visualization
    transaction_details = []
    for _, row in combined_df.iterrows():
        try:
            trans_data = json.loads(row['transaction_data'])
            if trans_data:
                trans_data['date'] = row['date']
                trans_data['state'] = row['state']
                trans_data['category'] = row['category']
                trans_data['timestamp'] = row['timestamp']
                transaction_details.append(trans_data)
        except:
            continue
    
    trans_df = pd.DataFrame(transaction_details)
    
    # Create visualizations
    plt.style.use('default')
    fig, axes = plt.subplots(2, 3, figsize=(18, 12))
    fig.suptitle('SmartSwift Transaction Analysis Dashboard', fontsize=16, fontweight='bold')
    
    # 1. Daily transaction volume
    daily_counts = combined_df.groupby('date').size()
    axes[0,0].bar(daily_counts.index, daily_counts.values, color='skyblue', edgecolor='navy')
    axes[0,0].set_title('Daily Transaction Volume')
    axes[0,0].set_ylabel('Number of Transactions')
    axes[0,0].tick_params(axis='x', rotation=45)
    
    # 2. Transaction states
    state_counts = combined_df['state'].value_counts()
    axes[0,1].pie(state_counts.values, labels=state_counts.index, autopct='%1.1f%%')
    axes[0,1].set_title('Transaction States')
    
    # 3. Categories
    category_counts = combined_df['category'].value_counts()
    axes[0,2].bar(category_counts.index, category_counts.values, color='lightgreen')
    axes[0,2].set_title('Transaction Categories')
    axes[0,2].tick_params(axis='x', rotation=45)
    
    # 4. Currency pairs (if available)
    if not trans_df.empty and 'send_currency' in trans_df and 'receive_currency' in trans_df:
        currency_pairs = trans_df[trans_df['send_currency'] != ''].groupby(['send_currency', 'receive_currency']).size().head(10)
        if not currency_pairs.empty:
            pair_labels = [f"{pair[0]}→{pair[1]}" for pair in currency_pairs.index]
            axes[1,0].barh(range(len(currency_pairs)), currency_pairs.values, color='orange')
            axes[1,0].set_yticks(range(len(currency_pairs)))
            axes[1,0].set_yticklabels(pair_labels)
            axes[1,0].set_title('Top Currency Pairs')
    
    # 5. Country corridors (if available)
    if not trans_df.empty and 'sender_country' in trans_df and 'receiver_country' in trans_df:
        country_pairs = trans_df[trans_df['sender_country'] != ''].groupby(['sender_country', 'receiver_country']).size().head(10)
        if not country_pairs.empty:
            corridor_labels = [f"{pair[0]}→{pair[1]}" for pair in country_pairs.index]
            axes[1,1].barh(range(len(country_pairs)), country_pairs.values, color='lightcoral')
            axes[1,1].set_yticks(range(len(country_pairs)))
            axes[1,1].set_yticklabels(corridor_labels)
            axes[1,1].set_title('Top Country Corridors')
    
    # 6. Hourly pattern
    combined_df['hour'] = pd.to_datetime(combined_df['timestamp']).dt.hour
    hourly_pattern = combined_df.groupby('hour').size()
    axes[1,2].plot(hourly_pattern.index, hourly_pattern.values, marker='o', linewidth=2)
    axes[1,2].set_title('Hourly Transaction Pattern')
    axes[1,2].set_xlabel('Hour of Day')
    axes[1,2].grid(True, alpha=0.3)
    
    plt.tight_layout()
    #plt.show()
    plt.savefig("Transaction-output_plot.png")
    
    # Final summary
    print(f"\n{'='*80}")
    print("FINAL SUMMARY")
    print(f"{'='*80}")
    print(f"Total transactions analyzed: {len(combined_df):,}")
    print(f"Date range: {min(combined_df['date'])} to {max(combined_df['date'])}")
    print(f"Most active day: {daily_counts.idxmax()} ({daily_counts.max():,} transactions)")
    print(f"Most common state: {combined_df['state'].mode().iloc[0]}")
    print(f"Most common category: {combined_df['category'].mode().iloc[0]}")
    
    if not trans_df.empty:
        print(f"Currency transactions: {len(trans_df[trans_df['send_currency'] != ''])}")
        if 'transaction_status' in trans_df:
            success_rate = (trans_df['transaction_status'] == 'SUCCESS').sum() / len(trans_df) * 100
            print(f"Success rate: {success_rate:.1f}%")

else:
    print("No data found for visualization")

# Stop Spark session
spark.stop()
print(f"\n{'='*80}")
print("ANALYSIS COMPLETED!")
print(f"{'='*80}")
