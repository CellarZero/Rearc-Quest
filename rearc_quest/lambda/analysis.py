import json
import pandas as pd
import boto3
import gzip
from io import BytesIO, StringIO

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    # Define S3 paths
    pr_data_path = "s3://rearcquestv2/bls-data/pr.data.0.Current"
    population_path = "s3://rearcquestv2/population-data/population.json"
    
    # Load BLS time-series data
    pr_dtypes = {
        'series_id': str,
        'year': int,
        'period': str,
        'value': float,
        'footnote_codes': str
    }
    response = s3.get_object(Bucket='rearcquestv2', Key='bls-data/pr.data.0.Current')
    compressed_body = response['Body'].read()
    decompressed_body = gzip.decompress(compressed_body).decode('utf-8')
    pr_df = pd.read_csv(StringIO(decompressed_body), sep='\t', dtype=pr_dtypes)

    # pr_df = pd.read_csv(
    #     pr_data_path,
    #     sep='\t',
    #     dtype=pr_dtypes,
    #     engine='python',
    #     compression='gzip'
    # )
    pr_df.columns = pr_df.columns.str.strip()
    pr_df = pr_df.map(lambda x: x.strip() if isinstance(x, str) else x)

    response = s3.get_object(Bucket='rearcquestv2', Key='population-data/population.json')
    population_json = json.loads(response['Body'].read())

    
    pop_df = pd.DataFrame(population_json['data'])
    pop_df = pop_df.astype({
        'ID Nation': str,
        'Nation': str,
        'ID Year': str,
        'Year': int,
        'Population': int,
        'Slug Nation': str
    })
    pop_df.columns = pop_df.columns.str.strip()
    pop_df = pop_df.map(lambda x: x.strip() if isinstance(x, str) else x)

    # Part 1: Population stats from 2013 to 2018
    filtered_pop_df = pop_df[(pop_df['Year'] >= 2013) & (pop_df['Year'] <= 2018)]
    mean_population = filtered_pop_df['Population'].mean()
    std_population = filtered_pop_df['Population'].std()
    print(f"Mean Population (2013-2018): {mean_population}")
    print(f"Standard Deviation of Population (2013-2018): {std_population}")

    # Part 2: Best year by summed value per series_id
    yearly_sum = pr_df.groupby(['series_id', 'year'], as_index=False)['value'].sum()
    best_years = yearly_sum.loc[yearly_sum.groupby('series_id')['value'].idxmax()]
    print("Best years per series_id:")
    print(best_years)

    # Part 3: Filtered join for a specific series_id and period
    filtered_pr = pr_df[(pr_df['series_id'] == 'PRS30006032') & (pr_df['period'] == 'Q01')].copy()
    merged_df = pd.merge(
        filtered_pr,
        pop_df[['Year', 'Population']],
        left_on='year',
        right_on='Year',
        how='left'
    )
    report_df = merged_df.dropna(subset=['Population']).drop(columns=['Year'])
    final_report = report_df[['series_id', 'year', 'period', 'value', 'Population']]

    print("Filtered Report for PRS30006032, Q01:")
    print(final_report)

    # Return all results as JSON
    return {
        "statusCode": 200,
        "body": json.dumps({
            "Message": 'Data processed and logged successfully',
            # "mean_population": round(mean_population),
            # "std_population": round(std_population),
            # "best_years": best_years.to_dict(orient='records'),
            # "filtered_report": final_report.to_dict(orient='records')
        })
    }
