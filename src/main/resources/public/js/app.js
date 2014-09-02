define([
    'angularAMD',
    'angular-route'
], function (angularAMD) {
    'use strict';

    var app = angular.module('app', ['ngRoute']);

    app.config(function ($routeProvider) {
        $routeProvider
            .when("/jobs", angularAMD.route({
                templateUrl: 'assets/partials/jobs.html',
                controller: 'JobCtrl',
                controllerUrl: 'controllers/job-ctrl'
            }))
            .when("/logs", angularAMD.route({
                templateUrl: 'assets/partials/logs.html',
                controller: 'LogCtrl',
                controllerUrl: 'controllers/log-ctrl'
            }))
            .when("/index", angularAMD.route({
                templateUrl: 'assets/partials/index.html'
            }))
            .otherwise({redirectTo: "/index"});
    });

    return angularAMD.bootstrap(app);
});