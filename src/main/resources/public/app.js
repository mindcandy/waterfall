requirejs.config({
    paths: {
        // external libraries
        'text': 'lib/text/text',
        'angular': 'lib/angular.min',
        'angular-route': 'lib/angular-route.min',
        'angularAMD': 'lib/angularAMD.min',
        'jquery': 'lib/jquery-1.11.1',
        'highcharts': 'lib/highcharts',

        // page modules
        'JobsPage': 'modules/JobsPage'
    },
    packages: [
        'JobsPage'
    ],
    shim : {
        'angularAMD': ['angular'],
        'angular-route' : ['angular'],
        'bootstrap' : [ 'jquery'],
        'highcharts' : {
            exports: 'Highcharts',
            deps: ['jquery']
        }
    }
});

define([
    'angularAMD',
    'angular-route',
    'jquery',
    'highcharts',
    'JobsPage'
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
