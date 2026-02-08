/**
 * Data fetching and caching service.
 * All data is pre-aggregated static JSON served from the same CloudFront origin.
 */
var DataService = (function () {
    'use strict';

    var cache = {};

    function loadJSON(url) {
        if (cache[url]) {
            return Promise.resolve(cache[url]);
        }

        return fetch(url)
            .then(function (response) {
                if (!response.ok) {
                    console.warn('Failed to load:', url, response.status);
                    return null;
                }
                return response.json();
            })
            .then(function (data) {
                if (data) {
                    cache[url] = data;
                }
                return data;
            })
            .catch(function (error) {
                console.error('Error loading:', url, error);
                return null;
            });
    }

    function clearCache() {
        cache = {};
    }

    return {
        loadJSON: loadJSON,
        clearCache: clearCache
    };
})();
