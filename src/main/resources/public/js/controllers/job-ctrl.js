define(['app'], function (app) {
    app.controller('JobCtrl', ['$http', function ($scope, $http) {
        $http.get('/assets/data/dash.json')
            .success(function (json) {
                $scope.families = json;
            });
    }]);
});
