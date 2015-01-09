requirejs.config({
    paths: {
        // external libraries
        'text': 'lib/text/text',
        'angular': 'lib/angular',
        'angular-route': 'lib/angular-route.min',
        'jquery': 'lib/jquery-1.11.1',
        'highcharts': 'lib/highcharts',

        // page modules
        'JobsPage': 'modules/JobsPage',
        'UserInterface': 'modules/UserInterface'
    },
    packages: [
        'JobsPage',
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
        }
    }
});

define([
    'angular',
    'UserInterface',
    'angular-route',
    'jquery',
    'highcharts'
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
