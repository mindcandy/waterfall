'use strict';

define([
    'angular',
    './controllers/StatsCtrl',
    'text!StatsPage/partials/stats.html'
], function (
    angular,
    StatsCtrl,
    StatsPartial
) {
    var StatsPage = angular.module('StatsPage', ['ngRoute']);

    StatsPage.config(function ($routeProvider) {
        $routeProvider
            .when("/stats", {
                template: StatsPartial,
                controller: 'StatsCtrl'
            })
            .otherwise({redirectTo: "/"});
    });


    // services

    // directives
    StatsPage.directive('highchart', function () {
        return {
            restrict: 'E',
            template: '<div></div>',
            replace: true,
            link: function (scope, element, attrs) {
                scope.$watch(function() { return attrs.chart; }, function() {
                    if(!attrs.chart) return;
                    var chart = JSON.parse(attrs.chart);
                    $(element[0]).highcharts(chart);
                });
            }
        }
    });

    // controllers
    StatsPage.controller('StatsCtrl', StatsCtrl);

    return StatsPage;
});
