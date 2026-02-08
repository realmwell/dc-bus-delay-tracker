/**
 * Leaflet map controller with choropleth ward rendering.
 */
var MapController = (function () {
    'use strict';

    var map, geojsonLayer;
    var wardClickCallback = null;
    var selectedLayer = null;
    var wardColors = {};  // Track current fill color per ward

    var DC_CENTER = [38.9072, -77.0369];
    var DC_ZOOM = 12;

    /**
     * Color scale based on on-time percentage.
     * 100% = bright green, 50% or below = deep red.
     * Scale compressed to 50-100% range so differences are visible.
     */
    function getColor(pctOnTime) {
        if (pctOnTime === null || pctOnTime === undefined) return '#cccccc';
        if (pctOnTime >= 95) return '#1a9850';
        if (pctOnTime >= 90) return '#66bd63';
        if (pctOnTime >= 85) return '#a6d96a';
        if (pctOnTime >= 80) return '#d9ef8b';
        if (pctOnTime >= 75) return '#fee08b';
        if (pctOnTime >= 70) return '#fdae61';
        if (pctOnTime >= 60) return '#f46d43';
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

    function onClick(e) {
        var layer = e.target;

        // Restore previous selection to its data-driven color
        if (selectedLayer && selectedLayer !== layer) {
            restoreLayerStyle(selectedLayer);
        }

        selectedLayer = layer;
        layer.setStyle(highlightStyle());
        // No bringToFront — it reorders SVG elements and breaks
        // subsequent taps on mobile (tap hits old top layer)

        var wardNum = layer.feature.properties.WARD;
        if (wardClickCallback) {
            wardClickCallback(wardNum);
        }
    }

    function onEachFeature(feature, layer) {
        layer.on({
            click: onClick
        });

        // Bind a permanent tooltip for the % label (updated in colorWards)
        layer.bindTooltip('', {
            permanent: true,
            direction: 'center',
            className: 'ward-pct-label'
        });
    }

    function init(containerId, wardGeoJSON) {
        map = L.map(containerId, {
            zoomControl: true,
            scrollWheelZoom: true,
            // Disable legacy tap handler — it interferes with ward clicks
            // on mobile by synthesizing events on wrong layers after bringToFront
            tap: false
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

        geojsonLayer.eachLayer(function (layer) {
            var wardNum = String(layer.feature.properties.WARD);
            var info = wardData[wardNum];
            var pctOnTime = info ? info.pct_on_time : null;
            var color = getColor(pctOnTime);

            wardColors[wardNum] = color;
            layer.setStyle({ fillColor: color });

            // Update the permanent tooltip with the on-time %
            if (info && pctOnTime !== null) {
                layer.setTooltipContent(Math.round(pctOnTime) + '%');
            } else {
                layer.setTooltipContent('');
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
