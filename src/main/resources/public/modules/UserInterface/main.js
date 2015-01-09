/*jslint browser: true, nomen: true */
/*global define, requirejs */

'use strict';

define([
    'angularAMD',
    'JobsPage'
], function (
    angular,
    JobsPage
) {
    console.log(angular);
    console.log('im in');

    var DEFAULT_STATE = 'jobs';

    var UserInterface = angular.module(
        'UserInterface',
        [
            'ui.router',
            'ngTable',
            'ngRoute',

            JobsPage.name
        ]
    );

    // controllers

    // directives
    
    // constants
    
    // services
    
    UserInterface.config([
        '$locationProvider', '$stateProvider', '$urlRouterProvider',
        function ($locationProvider, $stateProvider, $urlRouterProvider) {
            $locationProvider.html5Mode(true);
        }
    ]);

    // 500 - internal server error interceptor
    // TODO: move this directive to angular-tools
    UserInterface.factory('error500HttpInterceptor', ['$q', 'AlertService', function ($q, AlertService) {
        function formatErrorMessage (error, status) {
            return (error || '') + ' (status ' + status + ')';
        }

        return {
            responseError: function (rejection) {
                if (rejection.status === 500) {
                    AlertService.danger(formatErrorMessage(rejection.data.error, rejection.status));
                }
                return $q.reject(rejection);
            }
        };
    }]);

    UserInterface.config(['$httpProvider', function ($httpProvider) {
        $httpProvider.interceptors.push('error500HttpInterceptor');
    }]);

    // reload decorator
    UserInterface.config(['$provide', function ($provide) {
        $provide.decorator('$state', ['$delegate', '$stateParams', function ($delegate, $stateParams) {
            $delegate.forceReload = function () {
                return $delegate.go($delegate.current, $stateParams, {
                    reload: true,
                    inherit: false,
                    notify: true
                });
            };
            return $delegate;
        }]);
    }]);

    return UserInterface;

});
