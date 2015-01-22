requirejs.config({
    paths: {
        // external libraries
        'text': 'lib/text/text',
        'angular': 'lib/angular',
        'angular-route': 'lib/angular-route.min',
        'jquery': 'lib/jquery-1.11.1',
        'highcharts': 'lib/highcharts',
        'ng-table': 'lib/ng-table/ng-table',

        // page modules
        'JobsPage': 'modules/JobsPage',
        'StatsPage': 'modules/StatsPage',
        'UserInterface': 'modules/UserInterface'
    },
    packages: [
        'JobsPage',
        'StatsPage',
        'UserInterface'
    ],
    shim : {
        'angular': {
            exports: 'angular'
        },
        'angular-route' : ['angular'],
        'bootstrap' : [ 'jquery'],
        'highcharts' : {
            exports: 'Highcharts',
            deps: ['jquery']
        },
        'ng-table': ['angular']
    }
});

define([
    'angular',
    'UserInterface',
    'angular-route',
    'jquery',
    'highcharts',
    'ng-table'
], function (
    angular,
    UserInterface
) {
    angular.element(document).ready(function () {
        angular.bootstrap(document, [
            UserInterface.name
        ]);
    });
});
