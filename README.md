# Spotify_Data_Pipeline
Technologies used :
Spotipy Python library and various AWS services like Lambda for computation, S3 for storage, Cloudwatch for monitoring, and Athena to establish a comprehensive data pipeline

📌Extract Phase:
- Data extracted from Spotify API using Python Scripts and Spotipy.
- AWS Lambda deploys the extraction function.
- Automated extraction triggered by AWS CloudWatch.
- Raw data stored in an AWS S3 Bucket ("to_processed" folder).

📌Transform Phase:
- S3 triggers activate a Lambda function for new data.
- Data transformation executed by AWS Lambda.
- Transformed data organized in S3 Bucket by album, artist, and song; moved from "to_processed" to "processed".

📌Load Phase:
- Schema inferred using Glue Crawler for new S3 data.
- AWS Glue Data Catalog manages metadata.
- Final dataset queryable via Amazon Athena for in-depth analysis.

<img width="738" alt="Screenshot 2023-12-28 at 5 09 22 PM" src="https://github.com/RaunakSMatharu/Spotify_Data_Pipeline/assets/114725836/8fea570f-c14e-4e74-80a7-064281fb21a1">
