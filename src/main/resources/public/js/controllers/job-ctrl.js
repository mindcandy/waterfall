define(['app'], function (app) {
    app.controller('JobCtrl', ['$scope', '$http', '$timeout', function ($scope, $http, $timeout) {

        $scope.fetchJobs = function() {
            $scope.lastFetch = new Date();
            console.log($scope.lastFetch + ": fetching jobs...");
            $http.get('http://localhost:8080/jobs')
                .success(function (data) {
                    for (i = 0; i < data.jobs.length; i++) {
                        var jsonJob = data.jobs[i];
                        data.jobs[i] = fetchLogs(jsonJob)
                    }
                    $scope.jobs = data.jobs;
                    $scope.jobCount = data.count;
                })
                .error(function(data, status) {
                    $scope.status = status;
                    $scope.jobs = data.jobs || "Request failed";
                    console.error("failed to get jobs!");
                });

            $timeout(function() { $scope.fetchJobs(); }, $scope.refreshInterval * 5000);
        };

        $scope.jobStateButtonClass = function(job) {
            if(jobIsRunning(job)) return "btn-info";
            else if(jobIsDisabled(job) && jobHasErrorLogs(job)) return "btn-default";
            else if(jobIsDisabled(job)) return "btn-default";
            else if(jobHasErrorLogs(job)) return "btn-danger";
            else return "btn-success";
        };

        function fetchLogs(jsonJob) {
            $http.get('http://localhost:8080/logs?jobID=' + jsonJob.jobID + "&period=168")
                .success(function (data) {
                    jsonJob['logData'] = data;
                    jsonJob['status'] = jobStatus(jsonJob);
                })
                .error(function(data, status) {
                    console.error("failed to get logs for job " + jsonJob.jobID + "!");
                });
            return jsonJob;
        }

        function jobStatus(job) {
            if(jobIsRunning(job)) return "Running";
            else if(jobIsDisabled(job) && jobHasErrorLogs(job)) return "Disabled";
            else if(jobIsDisabled(job)) return "Disabled";
            else if(jobHasErrorLogs(job)) return "Failure";
            else return "Success";
        }

        function jobIsDisabled(job) {
            return job.enabled == false;
        }

        function jobIsRunning(job) {
            return job.logData != null && job.logData.logs[0] != null && job.logData.logs[0].endTime == null;
        }

        function jobHasErrorLogs(job) {
            return job.logData != null && job.logData.logs[0] != null && job.logData.logs[0].exception != null;
        }

        $scope.refreshInterval = 60;
        $scope.fetchJobs();
    }]);
});
