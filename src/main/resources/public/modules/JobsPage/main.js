'use strict';

define([
    'angularAMD',
    './controllers/JobsCtrl',
    'text!JobsPage/partials/jobs.html'
], function (
    angular,
    JobsCtrl,
    JobsPartial
) {

    var JobsPage = angular.module(
        'JobsPage',
        ['ui.router', 'ngTable', 'ngResource', 'ui.bootstrap']
    );

    JobsPage.config(['$stateProvider', function ($stateProvider) {
        $stateProvider.
            state('jobs', {
                url: '/jobs',
                template: JobsPartial,
                controller: 'JobsCtrl',
                strict: false
            });
    }]);

    // services

    // controllers
    JobsPage.controller('JobsCtrl', JobsCtrl);

    return JobsPage;

});
