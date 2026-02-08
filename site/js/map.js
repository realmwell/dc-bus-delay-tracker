/**
 * Leaflet map controller with choropleth ward rendering.
 */
var MapController = (function () {
    'use strict';

    var map, geojsonLayer;
    var wardClickCallback = null;
    var selectedLayer = null;
    var wardColors = {};  // Track current fill color per ward
    var wardLabels = {};  // Permanent % on-time labels per ward

    var DC_CENTER = [38.9072, -77.0369];
    var DC_ZOOM = 12;

    /**
     * Color scale based on on-time percentage.
     * 100% = bright green, lower = progressively red.
     */
    function getColor(pctOnTime) {
        if (pctOnTime === null || pctOnTime === undefined) return '#cccccc';
        if (pctOnTime >= 90) return '#1a9850';
        if (pctOnTime >= 80) return '#66bd63';
        if (pctOnTime >= 70) return '#a6d96a';
        if (pctOnTime >= 60) return '#fee08b';
        if (pctOnTime >= 50) return '#fdae61';
        if (pctOnTime >= 40) return '#f46d43';
        return '#d73027';
    }

    function defaultStyle() {
        return {
            fillColor: '#cccccc',
            weight: 2,
            opacity: 1,
            color: 'white',
            dashArray: '3',
            fillOpacity: 0.7
        };
    }

    function highlightStyle() {
        return {
            weight: 3,
            color: '#333',
            dashArray: '',
            fillOpacity: 0.85
        };
    }

    /**
     * Restore a layer to its data-driven color (not the grey default).
     */
    function restoreLayerStyle(layer) {
        var wardNum = String(layer.feature.properties.WARD);
        var color = wardColors[wardNum] || '#cccccc';
        layer.setStyle({
            fillColor: color,
            weight: 2,
            opacity: 1,
            color: 'white',
            dashArray: '3',
            fillOpacity: 0.7
        });
    }

    function onMouseOver(e) {
        var layer = e.target;
        if (layer !== selectedLayer) {
            layer.setStyle(highlightStyle());
            layer.bringToFront();
            if (selectedLayer) selectedLayer.bringToFront();
        }
    }

    function onMouseOut(e) {
        if (e.target !== selectedLayer) {
            restoreLayerStyle(e.target);
        }
    }

    function onClick(e) {
        var layer = e.target;

        // Restore previous selection to its data-driven color
        if (selectedLayer && selectedLayer !== layer) {
            restoreLayerStyle(selectedLayer);
        }

        selectedLayer = layer;
        layer.setStyle(highlightStyle());
        layer.bringToFront();

        var wardNum = layer.feature.properties.WARD;
        if (wardClickCallback) {
            wardClickCallback(wardNum);
        }
    }

    function onEachFeature(feature, layer) {
        layer.on({
            mouseover: onMouseOver,
            mouseout: onMouseOut,
            click: onClick
        });
    }

    function init(containerId, wardGeoJSON) {
        map = L.map(containerId, {
            zoomControl: true,
            scrollWheelZoom: true,
            tap: true
        }).setView(DC_CENTER, DC_ZOOM);

        L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
            maxZoom: 16,
            attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>'
        }).addTo(map);

        geojsonLayer = L.geoJson(wardGeoJSON, {
            style: defaultStyle,
            onEachFeature: onEachFeature
        }).addTo(map);

        map.fitBounds(geojsonLayer.getBounds(), { padding: [20, 20] });
    }

    function colorWards(wardData) {
        if (!geojsonLayer) return;

        // Remove old labels
        Object.keys(wardLabels).forEach(function (k) {
            if (wardLabels[k]) {
                map.removeLayer(wardLabels[k]);
            }
        });
        wardLabels = {};

        geojsonLayer.eachLayer(function (layer) {
            var wardNum = String(layer.feature.properties.WARD);
            var info = wardData[wardNum];
            var pctOnTime = info ? info.pct_on_time : null;
            var color = getColor(pctOnTime);

            wardColors[wardNum] = color;

            if (layer === selectedLayer) {
                // Keep highlight but update underlying color for restore
                layer.setStyle({ fillColor: color });
            } else {
                layer.setStyle({ fillColor: color });
            }

            // Add permanent label showing % on time
            if (info && pctOnTime !== null) {
                var center = layer.getBounds().getCenter();
                var label = L.marker(center, {
                    icon: L.divIcon({
                        className: 'ward-pct-label',
                        html: '<span>' + Math.round(pctOnTime) + '%</span>',
                        iconSize: [50, 28],
                        iconAnchor: [25, 14]
                    }),
                    interactive: false
                });
                label.addTo(map);
                wardLabels[wardNum] = label;
            }
        });
    }

    function onWardClick(callback) {
        wardClickCallback = callback;
    }

    function clearSelection() {
        if (selectedLayer) {
            restoreLayerStyle(selectedLayer);
            selectedLayer = null;
        }
    }

    function invalidateSize() {
        if (map) map.invalidateSize();
    }

    return {
        init: init,
        colorWards: colorWards,
        onWardClick: onWardClick,
        clearSelection: clearSelection,
        invalidateSize: invalidateSize
    };
})();
