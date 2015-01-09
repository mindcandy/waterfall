'use strict';

define([
    'angular',
    './controllers/JobsCtrl',
    'text!JobsPage/partials/jobs.html'
], function (
    angular,
    JobsCtrl,
    JobsPartial
) {
    var JobsPage = angular.module('JobsPage', ['ngRoute']);

    JobsPage.config(function ($routeProvider) {
        $routeProvider
            .when("/", {
                template: JobsPartial,
                controller: 'JobsCtrl'
            })
            .otherwise({redirectTo: "/"});
    });


    // services

    // controllers
    JobsPage.controller('JobsCtrl', JobsCtrl);

    return JobsPage;
});
