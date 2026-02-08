"""Aggregation logic: computes ward and route delay statistics for each time period."""

import statistics
import logging
from datetime import datetime, timedelta, timezone

from config import TIME_PERIODS, ON_TIME_MIN, ON_TIME_MAX

logger = logging.getLogger(__name__)


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


def aggregate_all_periods(historical_data, route_meta, s3):
    """Compute aggregations for all time periods and write to S3.

    Args:
        historical_data: list of (date_str, positions_list) sorted newest first
        route_meta: dict of route_id -> {name, line, wards}
        s3: S3IO instance
    """
    now = datetime.now(timezone.utc)

    for period_key, period_days in TIME_PERIODS.items():
        cutoff = now - timedelta(days=period_days)

        # Filter positions within this time period
        period_positions = []
        days_covered = 0
        has_historical = False
        for date_str, positions in historical_data:
            snap_date = datetime.strptime(date_str, '%Y-%m-%d').replace(tzinfo=timezone.utc)
            if snap_date >= cutoff:
                period_positions.extend(positions)
                days_covered += 1
                if date_str.endswith('-15') and positions and positions[0].get('vid', '').startswith('HIST'):
                    has_historical = True

        if not period_positions:
            logger.info(f'No data for period {period_key}, skipping')
            continue

        # Group deviations by ward and by route-within-ward
        ward_devs = {}
        ward_route_devs = {}

        for pos in period_positions:
            w = str(pos['ward'])
            r = pos['route']
            d = pos['dev']

            ward_devs.setdefault(w, []).append(d)
            ward_route_devs.setdefault(w, {}).setdefault(r, []).append(d)

        # Build ward summary
        ward_summary = {
            'period': period_key,
            'generated_at': now.isoformat(),
            'data_points': len(period_positions),
            'days_covered': days_covered,
            'has_historical': has_historical,
            'wards': {},
        }

        for w, devs in ward_devs.items():
            stats = compute_stats(devs)
            if not stats:
                continue

            # Find best/worst routes
            route_avgs = {}
            for r, r_devs in ward_route_devs.get(w, {}).items():
                if len(r_devs) >= 3:
                    route_avgs[r] = statistics.mean(r_devs)

            if route_avgs:
                stats['worst_route'] = max(route_avgs, key=route_avgs.get)
                stats['best_route'] = min(route_avgs, key=route_avgs.get)

            ward_summary['wards'][w] = stats

        s3.write_json(f'data/ward-summary-{period_key}.json', ward_summary)

        # Build per-ward route detail files
        for w in range(1, 9):
            w_str = str(w)
            routes_in_ward = ward_route_devs.get(w_str, {})

            route_list = []
            for r, devs in routes_in_ward.items():
                if r.startswith('HIST-'):
                    continue
                stats = compute_stats(devs)
                if stats:
                    meta = route_meta.get(r, {})
                    stats['route_id'] = r
                    stats['route_name'] = meta.get('name', r)
                    route_list.append(stats)

            route_list.sort(key=lambda x: x['avg_delay'], reverse=True)

            s3.write_json(f'data/ward-{w}-routes-{period_key}.json', {
                'ward': w,
                'period': period_key,
                'generated_at': now.isoformat(),
                'routes': route_list,
            })

        logger.info(f'Period {period_key}: {len(period_positions)} positions, {days_covered} days')
