define([
    'angularAMD',
    'angular-route'
], function (angularAMD) {
    'use strict';

    var app = angular.module('app', ['ngRoute']);

    app.config(function ($routeProvider) {
        $routeProvider
            .when("/", angularAMD.route({
                templateUrl: 'assets/partials/jobs.html',
                controller: 'JobCtrl',
                controllerUrl: 'controllers/job-ctrl'
            }))
            .otherwise({redirectTo: "/"});
    });

    return angularAMD.bootstrap(app);
});