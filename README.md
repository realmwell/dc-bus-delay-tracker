# DC Bus Delay Tracker

An interactive web app that visualizes Washington D.C. Metrobus schedule adherence by ward, built to make public transit performance data accessible and easy to understand.

**Live site:** [d2qzuvxfhipk64.cloudfront.net](https://d2qzuvxfhipk64.cloudfront.net)

## What It Does

The app displays a choropleth map of D.C.'s 8 wards, colored by bus on-time performance. Each ward shows its on-time percentage directly on the map. Click any ward to see detailed route-level statistics including average delay, on-time rate, and observation count.

Time period filters (1D, 1W, 1M, 3M, 6M, 1Y, 5Y) let users view performance trends over different windows.

## Architecture

Designed to run for under $0.15/month on AWS:

- **Frontend**: Static HTML/CSS/JS served from S3 via CloudFront (no framework, no build step)
- **Map**: Leaflet.js with GeoJSON ward boundaries from Open Data DC
- **Data Collection**: A single AWS Lambda function runs daily via EventBridge, fetching real-time bus positions from the WMATA API
- **Data Processing**: The Lambda maps each bus to a D.C. ward using pure Python ray-casting point-in-polygon (no external dependencies), then pre-aggregates all time period views as static JSON files
- **No database**: All computed views are static JSON served directly from CloudFront

## Methodology

### Real-Time Data (Daily Collection)
The Lambda function calls the WMATA `jBusPositions` API endpoint, which returns all active buses with a `Deviation` field (minutes ahead/behind schedule). Each bus position is mapped to a ward using its GPS coordinates and the D.C. ward boundary GeoJSON.

A bus is considered "on time" if its deviation is between -2 minutes (early) and +5 minutes (late), following WMATA's standard definition.

### Historical Data (System-Wide)
For longer time periods (1Y, 5Y), the app incorporates system-wide monthly bus on-time performance data from the WMATA Service Excellence Report, which covers July 2020 through November 2025. This data is system-wide (not per-ward), and is noted as "historical estimates" in the UI.

### Ward Mapping
Bus positions are assigned to wards using a ray-casting point-in-polygon algorithm against ward boundaries from Open Data DC (Wards from 2022). GeoJSON coordinates are [longitude, latitude]; the WMATA API returns separate Lat/Lon fields.

## Data Sources

- **[WMATA Developer API](https://developer.wmata.com/)** -- Real-time bus positions with schedule deviation (`jBusPositions` endpoint)
- **[WMATA Service Excellence Report](https://www.wmata.com/about/records/scorecard.cfm)** -- Monthly system-wide bus on-time performance data (historical)
- **[Open Data DC](https://opendata.dc.gov/datasets/wards-from-2022)** -- Ward boundary GeoJSON (Wards from 2022)

## Deployment

### Prerequisites
- AWS CLI configured
- SAM CLI installed
- A WMATA API key ([register here](https://developer.wmata.com/))

### Deploy
```bash
sam build && sam deploy --guided
```

You'll be prompted for your WMATA API key and alert email address.

### Upload Frontend
```bash
aws s3 sync site/ s3://YOUR-BUCKET-NAME/site/ --delete
```

### First Data Collection
```bash
aws lambda invoke --function-name dc-bus-tracker-collector /dev/stdout
```

## Cost Protection

- Lambda: single invocation per day, 512MB memory, 300s timeout
- S3: lifecycle rules expire raw data after 5 years
- CloudFront: PriceClass_100 (US/Canada/Europe only)
- Billing alarm at $5

## Tech Stack

- **Frontend**: Vanilla JS, Leaflet.js, CSS (mobile-first)
- **Backend**: Python 3.13 Lambda
- **Infrastructure**: AWS SAM/CloudFormation (S3, CloudFront, Lambda, EventBridge)
- **APIs**: WMATA Developer API

## Author

Built by [Max Greenberg](https://www.linkedin.com/in/maxwellgreenberg/) in 2026.
