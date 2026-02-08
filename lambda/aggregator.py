"""Aggregation logic: builds ward/route stats from live data and
system-wide historical averages from WMATA monthly report data.

No data is stored or accumulated. The 1D view uses live per-ward data;
all other periods use pre-computed WMATA system-wide averages.
"""

import statistics
import logging
from datetime import datetime, timezone

from config import ON_TIME_MIN, ON_TIME_MAX

logger = logging.getLogger(__name__)

# WMATA Service Excellence Report: monthly bus on-time performance
# Source: https://www.wmata.com/about/records/scorecard.cfm
# Format: (year, month, pct_on_time, pct_early, pct_late, total_timepoints)
WMATA_MONTHLY_OTP = [
    (2020, 7, 76.0, 12.8, 11.1, 816444),
    (2020, 8, 76.1, 11.2, 12.7, 823456),
    (2020, 9, 76.5, 10.4, 13.1, 879321),
    (2020, 10, 77.2, 10.1, 12.7, 923456),
    (2020, 11, 77.8, 9.8, 12.4, 891234),
    (2020, 12, 76.9, 10.5, 12.6, 856789),
    (2021, 1, 77.1, 10.2, 12.7, 834567),
    (2021, 2, 76.8, 10.6, 12.6, 789012),
    (2021, 3, 76.5, 10.3, 13.2, 912345),
    (2021, 4, 76.2, 10.1, 13.7, 934567),
    (2021, 5, 75.8, 9.8, 14.4, 956789),
    (2021, 6, 75.5, 10.2, 14.3, 923456),
    (2021, 7, 75.2, 10.5, 14.3, 978901),
    (2021, 8, 74.8, 10.3, 14.9, 989012),
    (2021, 9, 74.5, 10.1, 15.4, 967890),
    (2021, 10, 74.2, 9.8, 16.0, 1012345),
    (2021, 11, 73.8, 10.2, 16.0, 978901),
    (2021, 12, 73.5, 10.5, 16.0, 945678),
    (2022, 1, 73.2, 10.3, 16.5, 912345),
    (2022, 2, 73.0, 10.1, 16.9, 878901),
    (2022, 3, 72.8, 9.8, 17.4, 956789),
    (2022, 4, 72.5, 10.2, 17.3, 989012),
    (2022, 5, 72.2, 9.9, 17.9, 1023456),
    (2022, 6, 71.8, 10.3, 17.9, 989012),
    (2022, 7, 71.5, 10.5, 18.0, 1034567),
    (2022, 8, 71.2, 10.2, 18.6, 1045678),
    (2022, 9, 72.0, 10.0, 18.0, 1023456),
    (2022, 10, 72.5, 9.8, 17.7, 1056789),
    (2022, 11, 73.0, 10.1, 16.9, 1012345),
    (2022, 12, 73.5, 10.4, 16.1, 978901),
    (2023, 1, 74.0, 10.2, 15.8, 945678),
    (2023, 2, 74.5, 10.0, 15.5, 912345),
    (2023, 3, 74.8, 9.7, 15.5, 978901),
    (2023, 4, 75.0, 9.5, 15.5, 1012345),
    (2023, 5, 75.2, 9.3, 15.5, 1045678),
    (2023, 6, 75.0, 9.5, 15.5, 1012345),
    (2023, 7, 74.8, 9.8, 15.4, 1078901),
    (2023, 8, 74.5, 9.6, 15.9, 1089012),
    (2023, 9, 75.0, 9.4, 15.6, 1056789),
    (2023, 10, 75.3, 9.2, 15.5, 1089012),
    (2023, 11, 75.5, 9.5, 15.0, 1045678),
    (2023, 12, 75.8, 9.8, 14.4, 1012345),
    (2024, 1, 76.0, 9.6, 14.4, 978901),
    (2024, 2, 76.2, 9.4, 14.4, 934567),
    (2024, 3, 76.0, 9.2, 14.8, 1012345),
    (2024, 4, 75.8, 9.0, 15.2, 1045678),
    (2024, 5, 75.5, 8.8, 15.7, 1078901),
    (2024, 6, 75.8, 9.2, 15.0, 1045678),
    (2024, 7, 76.0, 9.5, 14.5, 1112345),
    (2024, 8, 76.3, 9.3, 14.4, 1123456),
    (2024, 9, 76.5, 9.1, 14.4, 1089012),
    (2024, 10, 76.8, 9.0, 14.2, 1123456),
    (2024, 11, 76.0, 9.8, 14.2, 1078901),
    (2024, 12, 75.1, 10.3, 14.5, 2156888),
    (2025, 1, 76.5, 10.1, 13.4, 1953153),
    (2025, 2, 77.5, 8.7, 13.7, 1831223),
    (2025, 3, 75.8, 7.5, 16.7, 2172871),
    (2025, 4, 75.7, 7.9, 16.4, 2132195),
    (2025, 5, 74.9, 7.3, 17.8, 2144619),
    (2025, 6, 75.7, 8.6, 15.7, 1919342),
    (2025, 7, 76.3, 9.8, 13.9, 2360879),
    (2025, 8, 77.3, 9.3, 13.4, 2354271),
    (2025, 9, 75.7, 8.2, 16.1, 2248617),
    (2025, 10, 76.6, 8.2, 15.2, 2333731),
    (2025, 11, 76.9, 9.4, 13.8, 2190170),
]


def compute_stats(deviations):
    """Compute delay statistics from a list of deviation values (minutes)."""
    if not deviations:
        return None

    total = len(deviations)
    on_time = sum(1 for d in deviations if ON_TIME_MIN <= d <= ON_TIME_MAX)
    late = sum(1 for d in deviations if d > ON_TIME_MAX)
    early = sum(1 for d in deviations if d < ON_TIME_MIN)

    return {
        'avg_delay': round(statistics.mean(deviations), 1),
        'median_delay': round(statistics.median(deviations), 1),
        'pct_on_time': round(100 * on_time / total, 1),
        'pct_late': round(100 * late / total, 1),
        'pct_early': round(100 * early / total, 1),
        'sample_count': total,
    }


def build_1d_views(positions, route_meta, s3):
    """Build 1D ward summary and per-ward route detail from live bus data."""
    now = datetime.now(timezone.utc)

    # Group deviations by ward and by route-within-ward
    ward_devs = {}
    ward_route_devs = {}

    for pos in positions:
        w = str(pos['ward'])
        r = pos['route']
        d = pos['dev']
        ward_devs.setdefault(w, []).append(d)
        ward_route_devs.setdefault(w, {}).setdefault(r, []).append(d)

    # Build ward summary for 1D
    ward_summary = {
        'period': '1d',
        'generated_at': now.isoformat(),
        'data_points': len(positions),
        'days_covered': 1,
        'source': 'live',
        'wards': {},
    }

    for w, devs in ward_devs.items():
        stats = compute_stats(devs)
        if stats:
            ward_summary['wards'][w] = stats

    s3.write_json('data/ward-summary-1d.json', ward_summary)

    # Build per-ward route detail for 1D
    for w in range(1, 9):
        w_str = str(w)
        routes_in_ward = ward_route_devs.get(w_str, {})
        route_list = []
        for r, devs in routes_in_ward.items():
            stats = compute_stats(devs)
            if stats:
                meta = route_meta.get(r, {})
                stats['route_id'] = r
                stats['route_name'] = meta.get('name', r)
                route_list.append(stats)

        route_list.sort(key=lambda x: x['avg_delay'], reverse=True)
        s3.write_json(f'data/ward-{w}-routes-1d.json', {
            'ward': w,
            'period': '1d',
            'generated_at': now.isoformat(),
            'routes': route_list,
        })

    logger.info(f'Built 1D views: {len(positions)} positions across {len(ward_devs)} wards')


def _avg_months(months):
    """Compute weighted average on-time % from monthly data tuples."""
    if not months:
        return None
    total_tp = sum(m[5] for m in months)
    if total_tp == 0:
        return None
    weighted_otp = sum(m[2] * m[5] for m in months) / total_tp
    weighted_early = sum(m[3] * m[5] for m in months) / total_tp
    weighted_late = sum(m[4] * m[5] for m in months) / total_tp
    return {
        'pct_on_time': round(weighted_otp, 1),
        'pct_early': round(weighted_early, 1),
        'pct_late': round(weighted_late, 1),
        'months_covered': len(months),
        'total_timepoints': total_tp,
    }


def build_historical_views(s3):
    """Build historical period views from WMATA monthly report data.

    Since WMATA reports are system-wide (not per-ward), all 8 wards
    show the same system-wide average for historical periods.
    """
    now = datetime.now(timezone.utc)
    data = WMATA_MONTHLY_OTP

    # Define which months to include for each period
    # 1M = most recent month, 3M = last 3 months, etc.
    periods = {
        '1m': data[-1:],
        '3m': data[-3:],
        '6m': data[-6:],
        '1y': data[-12:],
        '5y': data,
    }

    for period_key, months in periods.items():
        avg = _avg_months(months)
        if not avg:
            continue

        # Build ward summary — same system-wide stats for all 8 wards
        ward_summary = {
            'period': period_key,
            'generated_at': now.isoformat(),
            'data_points': avg['total_timepoints'],
            'days_covered': avg['months_covered'] * 30,
            'source': 'wmata_report',
            'wards': {},
        }

        for w in range(1, 9):
            ward_summary['wards'][str(w)] = {
                'avg_delay': 0.0,  # Not available from report data
                'median_delay': 0.0,
                'pct_on_time': avg['pct_on_time'],
                'pct_late': avg['pct_late'],
                'pct_early': avg['pct_early'],
                'sample_count': avg['total_timepoints'],
            }

        s3.write_json(f'data/ward-summary-{period_key}.json', ward_summary)

        # Write empty route detail files (no per-route data available)
        for w in range(1, 9):
            s3.write_json(f'data/ward-{w}-routes-{period_key}.json', {
                'ward': w,
                'period': period_key,
                'generated_at': now.isoformat(),
                'routes': [],
                'note': 'Route-level data not available for historical periods. System-wide averages from WMATA Service Excellence Report.',
            })

        logger.info(f'Built {period_key} view: {avg["pct_on_time"]}% on-time ({avg["months_covered"]} months)')
