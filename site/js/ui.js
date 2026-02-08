/**
 * UI rendering: ward detail panel, route tables, info bar.
 */
var UI = (function () {
    'use strict';

    var panel = null;
    var panelContent = null;
    var panelTitle = null;

    function initElements() {
        if (!panel) {
            panel = document.getElementById('detail-panel');
            panelContent = document.getElementById('panel-content');
            panelTitle = document.getElementById('panel-title');
        }
    }

    function showPanel() {
        initElements();
        panel.classList.remove('hidden');
        MapController.invalidateSize();
    }

    function hidePanel() {
        initElements();
        panel.classList.add('hidden');
        MapController.invalidateSize();
    }

    function getDelayClass(avgDelay) {
        if (avgDelay > 5) return 'delay-bad';
        if (avgDelay > 2) return 'delay-warn';
        return 'delay-ok';
    }

    function renderWardDetail(wardNumber, wardStats, routes, summaryMeta) {
        initElements();
        panelTitle.textContent = 'Ward ' + wardNumber;

        var html = '';

        if (wardStats) {
            html += '<div class="ward-stats">';
            html += '<div class="stat">' +
                '<span class="stat-value">' + wardStats.pct_on_time.toFixed(0) + '%</span>' +
                '<span class="stat-label">On Time</span></div>';
            html += '<div class="stat">' +
                '<span class="stat-value ' + getDelayClass(wardStats.avg_delay) + '">' +
                wardStats.avg_delay.toFixed(1) + ' min</span>' +
                '<span class="stat-label">Avg Delay</span></div>';
            html += '<div class="stat">' +
                '<span class="stat-value">' + wardStats.sample_count.toLocaleString() + '</span>' +
                '<span class="stat-label">Observations</span></div>';
            html += '</div>';
        }

        if (summaryMeta && summaryMeta.days_covered) {
            html += '<p class="data-note">' + summaryMeta.days_covered + ' day' +
                (summaryMeta.days_covered !== 1 ? 's' : '') + ' of data';
            if (summaryMeta.has_historical) {
                html += ' (includes WMATA historical estimates)';
            }
            html += '</p>';
        }

        html += '<div class="routes-header">Routes in Ward ' + wardNumber + '</div>';

        if (routes && routes.length > 0) {
            html += '<div class="table-wrap">';
            html += '<table class="route-table">';
            html += '<thead><tr>' +
                '<th>Route</th>' +
                '<th>Avg Delay</th>' +
                '<th>On Time</th>' +
                '<th>Obs.</th>' +
                '</tr></thead>';
            html += '<tbody>';

            for (var i = 0; i < routes.length; i++) {
                var r = routes[i];
                var cls = getDelayClass(r.avg_delay);
                html += '<tr>' +
                    '<td class="route-id">' + escapeHtml(r.route_id) + '</td>' +
                    '<td class="' + cls + '">' + r.avg_delay.toFixed(1) + ' min</td>' +
                    '<td>' + r.pct_on_time.toFixed(0) + '%</td>' +
                    '<td>' + r.sample_count.toLocaleString() + '</td>' +
                    '</tr>';
            }

            html += '</tbody></table></div>';
        } else {
            html += '<p class="no-data">No route data available for this period.</p>';
        }

        panelContent.innerHTML = html;
    }

    function updateLastUpdated(isoTimestamp) {
        if (!isoTimestamp) return;
        var date = new Date(isoTimestamp);
        var el = document.getElementById('last-updated');
        el.textContent = 'Updated: ' + date.toLocaleDateString('en-US', {
            month: 'short', day: 'numeric', year: 'numeric'
        });
    }

    function showNoData() {
        initElements();
        panelContent.innerHTML = '<p class="no-data">No data available yet. Check back after the first data collection run.</p>';
    }

    function escapeHtml(str) {
        var div = document.createElement('div');
        div.textContent = str;
        return div.innerHTML;
    }

    return {
        showPanel: showPanel,
        hidePanel: hidePanel,
        renderWardDetail: renderWardDetail,
        updateLastUpdated: updateLastUpdated,
        showNoData: showNoData
    };
})();
