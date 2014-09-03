define(['app'], function (app) {
    app.controller('JobCtrl', ['$scope', '$http', function ($scope, $http) {

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

        function fetchLogs(jsonJob) {
            $http.get('http://localhost:8080/logs?jobID=' + jsonJob.jobID + "&period=168")
                .success(function (data) {
                    jsonJob['logData'] = data;
                })
                .error(function(data, status) {
                    console.error("failed to get logs for job " + jsonJob.jobID + "!");
                });
            return jsonJob;
        }

        $scope.jobStateButtonClass = function(job) {
            if(jobIsRunning(job)) return "btn-info";
            else if(jobIsDisabled(job) && jobHasErrorLogs(job)) return "btn-warning";
            else if(jobIsDisabled(job)) return "btn-default";
            else if(jobHasErrorLogs(job)) return "btn-danger";
            else return "btn-success";
        };

        $scope.jobStateButtonText = function(job) {
            if(jobIsRunning(job)) return "Running";
            else if(jobIsDisabled(job) && jobHasErrorLogs(job)) return "Disabled/Errors";
            else if(jobIsDisabled(job)) return "Disabled";
            else if(jobHasErrorLogs(job)) return "Failed";
            else return "Completed";
        };

        function jobIsDisabled(job) {
            return job.enabled == false;
        }

        function jobIsRunning(job) {
            return job.logData != null && job.logData.logs[0] != null && job.logData.logs[0].endTime == null;
        }

        function jobHasErrorLogs(job) {
            return job.logData != null && job.logData.logs[0] != null && job.logData.logs[0].exception != null;
        }
    }]);
});
