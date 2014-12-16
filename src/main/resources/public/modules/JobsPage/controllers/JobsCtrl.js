define([], function () {
    var JobsCtrl = function ($scope, $http, $timeout) {
        $scope.jobLogsBeingViewed = []; // a list of jobs logs being viewed (to be 'reopened' after refresh)
        $scope.refreshInterval = 300000; // refresh time in milliseconds (5min)

        $scope.$watch("filter.$", function () {
            $scope.tableParams.reload();
        });

        $scope.tableParams = new ngTableParams({
            //page: 1,                     // show first page
            count: $scope.jobs.length,    // count per page
            sorting: {
                name: 'asc'
            }
        }, {
            counts:[],
            total: $scope.jobs.length,    // length of data
            getData: function($defer, params) {
                var filteredData = $filter('filter')($scope.jobs, $scope.filter);
                //console.log('getting data', $scope.jobs);
                //console.log(JSON.stringify(params.sorting()) + " " + params.orderBy());
                var orderedData = params.sorting() ? $filter('orderBy')(filteredData, params.orderBy()) : filteredData;
                //console.log(orderedData);
                //var paginatedData = orderedData.slice((params.page() - 1) * params.count(), params.page() * params.count());
                $defer.resolve(orderedData);
            }
        });

        /** fetch all jobs and their logs */
        $scope.fetchJobs = function() {
            $scope.lastFetch = new Date();
            console.log($scope.lastFetch + ": fetching jobs...");
            $http.get('/jobs')
                .success(function (data) {
                    for (i = 0; i < data.jobs.length; i++) {
                        var jsonJob = data.jobs[i];
                        data.jobs[i] = fetchLogs(jsonJob)
                    }
                    $scope.jobs = data.jobs;
                    $scope.jobCount = data.count;
                    $scope.tableParams.reload();
                })
                .error(function(data, status) {
                    $scope.status = status;
                    $scope.jobs = data.jobs || "Request failed";
                    console.error("failed to get jobs!");
                });
            // perform refresh after interval
            $timeout(function() { $scope.fetchJobs(); }, $scope.refreshInterval);
        };

        /* fetch the logs for the given job */
        function fetchLogs(jsonJob) {
            $http.get('/logs?jobid=' + jsonJob.jobID + "&period=168&limit=5")
                .success(function (data) {
                    jsonJob['logData'] = data;
                    jsonJob['status'] = jobStatus(jsonJob);
                })
                .error(function(data, status) {
                    console.error("failed to get logs for job " + jsonJob.jobID + "!");
                });
            return jsonJob;
        }

        /** job status button display */
        $scope.jobStateButtonClass = function(job) {
            if(jobIsRunning(job)) return "btn-info";
            else if(jobIsDisabled(job) && jobHasErrorLogs(job)) return "btn-default";
            else if(jobIsDisabled(job)) return "btn-default";
            else if(jobHasErrorLogs(job)) return "btn-danger";
            else if(jobIsNeverRun(job)) return "btn-primary";
            else return "btn-success";
        };

        /* tracks jobs whose logs are being viewed so that those logs are again viewed after refresh */
        $scope.jobLogClicked = function(job) {
            var index = $scope.jobLogsBeingViewed.indexOf(job.jobID);
            if (index == -1) $scope.jobLogsBeingViewed.push(job.jobID);
            else $scope.jobLogsBeingViewed.splice(index, 1);
        };

        /* determines whether or not the given job logs are viewable */
        $scope.jobLogViewable = function(job) {
             return $scope.jobLogsBeingViewed.indexOf(job.jobID) > -1 && job.logData != null && job.logData.count != 0;
        };

        function jobStatus(job) {
            if(jobIsRunning(job)) return "Running";
            else if(jobIsDisabled(job) && jobHasErrorLogs(job)) return "Disabled";
            else if(jobIsDisabled(job)) return "Disabled";
            else if(jobHasErrorLogs(job)) return "Failure";
            else if(jobIsNeverRun(job)) return "Never Run";
            else return "Success";
        }

        function jobIsNeverRun(job) {
            return job.logData == null || job.logData.logs[0] == null
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

        /* run initial lookup */
        $scope.fetchJobs();
    };

    return ['$scope', '$http', '$timeout', JobsCtrl]
});
