/**
 * Main application controller.
 * Coordinates data loading, map rendering, and UI updates.
 */
(function () {
    'use strict';

    var DATA_BASE = '/data';

    var state = {
        currentPeriod: '1d',
        selectedWard: null,
        wardSummary: null
    };

    function init() {
        DataService.loadJSON(DATA_BASE + '/dc-wards.geojson')
            .then(function (geojson) {
                if (!geojson) {
                    console.error('Failed to load ward GeoJSON');
                    return;
                }

                MapController.init('map', geojson);
                MapController.onWardClick(handleWardClick);

                setupFilters();
                document.getElementById('panel-close').addEventListener('click', closePanel);

                return loadPeriodData(state.currentPeriod);
            })
            .then(function () {
                return DataService.loadJSON(DATA_BASE + '/last-updated.json');
            })
            .then(function (status) {
                if (status) {
                    UI.updateLastUpdated(status.last_run);
                }
            });
    }

    function loadPeriodData(period) {
        state.currentPeriod = period;
        DataService.clearCache();

        return DataService.loadJSON(DATA_BASE + '/ward-summary-' + period + '.json')
            .then(function (summary) {
                state.wardSummary = summary;
                if (summary && summary.wards) {
                    MapController.colorWards(summary.wards);
                }

                if (state.selectedWard) {
                    return loadWardRoutes(state.selectedWard);
                }
            });
    }

    function setupFilters() {
        var buttons = document.querySelectorAll('.filter-btn');
        for (var i = 0; i < buttons.length; i++) {
            buttons[i].addEventListener('click', function () {
                var active = document.querySelector('.filter-btn.active');
                if (active) active.classList.remove('active');
                this.classList.add('active');
                loadPeriodData(this.getAttribute('data-period'));
            });
        }
    }

    function handleWardClick(wardNumber) {
        state.selectedWard = wardNumber;
        UI.showPanel();
        loadWardRoutes(wardNumber);
    }

    function loadWardRoutes(wardNumber) {
        var url = DATA_BASE + '/ward-' + wardNumber + '-routes-' + state.currentPeriod + '.json';
        return DataService.loadJSON(url)
            .then(function (routeData) {
                var wardStats = null;
                if (state.wardSummary && state.wardSummary.wards) {
                    wardStats = state.wardSummary.wards[String(wardNumber)];
                }
                var routes = routeData ? routeData.routes : [];
                var meta = state.wardSummary ? {
                    days_covered: state.wardSummary.days_covered,
                    source: state.wardSummary.source
                } : null;
                UI.renderWardDetail(wardNumber, wardStats, routes, meta);
            });
    }

    function closePanel() {
        state.selectedWard = null;
        UI.hidePanel();
        MapController.clearSelection();
    }

    document.addEventListener('DOMContentLoaded', init);
})();
