/*jslint browser: true, nomen: true */
/*global define, requirejs */

'use strict';

define([
    'angular',
    'JobsPage'
], function (
    angular,
    JobsPage
) {

    var DEFAULT_STATE = 'jobs';

    var UserInterface = angular.module(
        'UserInterface',
        [
            'ngRoute',
            JobsPage.name
        ]
    );

    // controllers

    // directives
    
    // constants
    
    // services
    
    UserInterface.config([
        '$locationProvider',
        function ($locationProvider) {
            $locationProvider.html5Mode(true);
        }
    ]);

    return UserInterface;

});
