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
            .otherwise({redirectTo: "/jobs"});
    });

    return angularAMD.bootstrap(app);
});