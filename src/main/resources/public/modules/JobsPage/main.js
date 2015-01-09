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

    // JobsPage.config(['$stateProvider', function ($stateProvider) {
    //     $stateProvider.
    //         state('jobs', {
    //             url: '/jobs',
    //             template: JobsPartial,
    //             controller: 'JobsCtrl',
    //             strict: false
    //         });
    // }]);

    JobsPage.config(function ($routeProvider) {
        $routeProvider
            .when("/", {
                template: JobsPartial,
                controller: 'JobsCtrl'
                // controllerUrl: 'controllers/job-ctrl'
            })
            .otherwise({redirectTo: "/"});
    });


    // services

    // controllers
    JobsPage.controller('JobsCtrl', JobsCtrl);

    return JobsPage;
});
