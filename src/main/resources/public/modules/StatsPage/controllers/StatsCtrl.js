define([], function () {
    var StatsCtrl = function($scope, $http, $timeout) {
        $scope.jobLogsBeingViewed = []; // a list of jobs logs being viewed (to be 'reopened' after refresh)
        $scope.refreshInterval = 300000; // refresh time in milliseconds (5min)

        /** fetch all jobs and their logs */
        $scope.fetchJobs = function () {
            $scope.lastFetch = moment();
            $http.get('/jobs')
                .success(function (data) {
                    for (var i = 0, len = data.jobs.length; i < len; i++) {
                        var jsonJob = data.jobs[i];
                        data.jobs[i] = fetchLogs(jsonJob)
                    }
                    $scope.jobs = data.jobs;
                    $scope.jobCount = data.count;
                })
                .error(function (data, status) {
                    $scope.status = status;
                    $scope.jobs = data.jobs || "Request failed";
                    console.error("failed to get jobs!");
                });
            // perform refresh after interval
            $timeout(function () {
                $scope.fetchJobs();
            }, $scope.refreshInterval);
        };

        /* fetch the logs for the given job */
        function fetchLogs(jsonJob) {
            $http.get('/logs?jobid=' + jsonJob.jobID + "&period=168&limit=5")
                .success(function (data) {
                    jsonJob.logData = data;
                    jsonJob.status = jobStatus(jsonJob);
                })
                .error(function (data, status) {
                    console.error("failed to get logs for job " + jsonJob.jobID + "!");
                });
            return jsonJob;
        }

        /** job status button display */
        $scope.jobStateButtonClass = function (job) {
            if (jobIsRunning(job)) { return "btn-info"; }
            else if (jobIsDisabled(job) && jobHasErrorLogs(job)) { return "btn-default"; }
            else if (jobIsDisabled(job)) { return "btn-default"; }
            else if (jobHasErrorLogs(job)) { return "btn-danger"; }
            else if (jobIsNeverRun(job)) { return "btn-primary"; }
            else { return "btn-success"; }
        };

        /* tracks jobs whose logs are being viewed so that those logs are again viewed after refresh */
        $scope.jobLogClicked = function (job) {
            var index = $scope.jobLogsBeingViewed.indexOf(job.jobID);
            if (index === -1) { $scope.jobLogsBeingViewed.push(job.jobID); }
            else { $scope.jobLogsBeingViewed.splice(index, 1); }
        };

        /* determines whether or not the given job logs are viewable */
        $scope.jobLogViewable = function (job) {
             return $scope.jobLogsBeingViewed.indexOf(job.jobID) > -1 && job.logData !== null && job.logData.count !== 0;
        };

        function jobStatus(job) {
            if (jobIsRunning(job)) { return "Running"; }
            else if (jobIsDisabled(job) && jobHasErrorLogs(job)) { return "Disabled"; }
            else if (jobIsDisabled(job)) { return "Disabled"; }
            else if (jobHasErrorLogs(job)) { return "Failure"; }
            else if (jobIsNeverRun(job)) { return "Never Run"; }
            else { return "Success"; }
        }

        function jobIsNeverRun(job) {
            return job.logData === null || job.logData[0] === null;
        }

        function jobIsDisabled(job) {
            return job.enabled === false;
        }

        function jobIsRunning(job) {
            if (job.logData === null) { return false; }
            else if (job.logData.length === 0) { return false; }
            else { return job.logData[0] !== null && job.logData[0].endTime === null; }
        }

        function jobHasErrorLogs(job) {
            if (job.logData === null) { return false; }
            else if (job.logData.length === 0) { return false; }
            else { return job.logData[0] !== null && job.logData[0].exception !== null; }
        }

        /* Create the JSON for displaying the chart */
        function columnChartFormatting(seriesData, xAxisDateRange, xAxisLabelStep, isLegend, height, width) {
            return {
                chart: {
                    type: 'column',
                    height: height,
                    width: width,
                    zoomType: 'x'
                },

                title: {
                    text: null
                },

                legend: {
                    enabled: isLegend,
                    align: 'right',
                    verticalAlign: 'middle',
                    layout: 'vertical',
                    itemStyle: {
                        fontWeight: 'normal',
                        fontFamily: '"Helvetica Neue",Helvetica,Arial,sans-serif'
                    },
                    itemHoverStyle: {
                        fontWeight: 'bold'
                    },
                    itemMarginTop: 4,
                    width: 220
                },

                xAxis: {
                    type: 'datetime',
                    categories: xAxisDateRange,
                    title: {
                        text: 'Day',
                        style: {
                            fontWeight: 'bold',
                            fontFamily: '"Helvetica Neue",Helvetica,Arial,sans-serif'
                        }
                    },
                    ordinal: false,
                    labels: {
                        //format: '{ value: %Y-%m-%d }',
                        format: '{value:%Y-%m-%d}',
                        staggerLines: 1,
                        step: xAxisLabelStep,
                        rotation: -40
                    }
                },

                yAxis: {
                    type: 'datetime',
                    dateTimeLabelFormats: { // force all formats to be hour:minute:second
                        second: '%H:%M:%S',
                        minute: '%H:%M:%S',
                        hour: '%H:%M:%S',
                        day: '%H:%M:%S',
                        week: '%H:%M:%S',
                        month: '%H:%M:%S',
                        year: '%H:%M:%S'
                    },
                    title: {
                        text: 'Run Length (mins)',
                        style: {
                            fontWeight: 'bold',
                            fontFamily: '"Helvetica Neue",Helvetica,Arial,sans-serif'
                        }
                    },
                    gridLineColor: '#E5E4E2'
                },

                tooltip: {
                    xDateformat: '%Y-%m-%d',
                    formatter: function () {
                        return '<b>' + this.series.name + '</b><br/>' + Highcharts.dateFormat('%Y-%m-%d', this.x) + ': ' + this.y;
                    }
                },

                credits: {
                    enabled: false
                },

                series: seriesData
            };
        }

        /* Get all dates between a start and end date */
        function getDateRange(startDate, stopDate) {
            var dateArray = [];
            var currentDate = startDate.clone();
            while (!stopDate.isBefore(currentDate)) {
                dateArray.push(currentDate.valueOf());
                currentDate = currentDate.add(1, 'days');
            }
            return dateArray;
        }

        /* Organise data into chart x axis categories */
        function getDropRuntimeSeriesData(seriesData) {
            var minDate = moment();
            for (var i = 0, iLen = seriesData.length; i < iLen; i++) {
                for (var j = 0, jLen = seriesData[i].data.length; j < jLen; j++) {
                    if (minDate.isAfter(seriesData[i].data[j][0])) {
                        minDate = moment(seriesData[i].data[j][0]);
                    }
                }
            }

            // Get range of categories to be shown in graph
            var dateRange = getDateRange(minDate, moment());

            // Assign each series point to a category ID
            for (var k = 0, kLen = seriesData.length; k < kLen; k++) {
                for (var m = 0, mLen = seriesData[k].data.length; m < mLen; m++) {
                    var index = -1;
                    for (var n = 0, nLen = dateRange.length; n < nLen; n++) {
                        if (moment(dateRange[n]).format('YYYY-MM-DD') === seriesData[k].data[m][0]) {
                            index = n;
                            break;
                        }
                    }
                    seriesData[k].data[m][0] = index;
                }
            }

            return { seriesData: seriesData, dateRange: dateRange };
        }

        /* Organise data into 1 value for each date */
        function getTotalRuntimeSeriesData(seriesData) {
            var minDate = moment();
            if (seriesData.length > 0) {
                // seriesData is sorted, so minDate is first in array
                minDate = moment(seriesData[0].date, "YYYY-MM-DD");
            } else {
                console.error("No logs present");
            }

            // Get range of categories to be shown in graph
            var dateRange = getDateRange(minDate, moment());

            // Add empty entries for non-existant dates
            for (var i = 0, iLen = i < dateRange.length; i < iLen; i++) {
                var containsDate = false;
                var thisDate = moment(dateRange[i])
                for (var j = 0, jLen = seriesData.length; j < jLen; j++) {
                    if (thisDate.format('YYYY-MM-DD') === seriesData[j].date) {
                        containsDate = true;
                        break;
                    }
                }
                if (!containsDate) {
                    seriesData.push({ date: thisDate.format('YYYY-MM-DD'), runtime: 0 });
                }
            }

            // Reformat as a flat array, as there is only 1 series
            seriesData = seriesData.sort(function (a,b) { return a.date.localeCompare(b.date); });
            var outputArray = [];
            for (var k = 0, kLen = seriesData.length; k < kLen; k++) {
                outputArray.push(seriesData[k].runtime);
            }

            return { seriesData: outputArray, dateRange: dateRange };
        }

        /* Get jobs data from API */
        function getDropRuntimesChart() {
            // Get all logs for all jobs, formatted to be accepted by Highcharts
            var dataArray = [];
            for (var i = 0, iLen = $scope.jobCount; i < iLen; i++) {
                var job = { name: $scope.jobs[i].name, data: [] };
                for (var j = 0, jLen = $scope.jobs[i].logData.length; j < jLen; j++) {
                    var logs = $scope.jobs[i].logData[j];
                    var start = moment(logs.startTime);
                    var end = moment(logs.endTime);
                    job.data.push([ start.format('YYYY-MM-DD'), end.valueOf() - start.valueOf() ]);
                }
                dataArray.push(job);
            }

            // Sort the series data by name (affects its order in the legend)
            var seriesData = dataArray.sort(function (a,b) { return a.name.localeCompare(b.name); });

            // Get the dates to be used as x axis categories and the series data
            var chartData = getDropRuntimeSeriesData(seriesData);

            // Create the chart object
            return columnChartFormatting(chartData.seriesData, chartData.dateRange, 1, true, 500, 1500);
        }

        /* Get jobs data from API */
        function getTotalRuntimesChart() {
            // Get all logs for all jobs, formatted to be accepted by Highcharts
            var dataArray = [];
            for (var i = 0, iLen = $scope.jobCount; i < iLen; i++) {
                for (var j = 0, jLen = $scope.jobs[i].logData.length; j < jLen; j++) {
                    var jobLogs = $scope.jobs[i].logData[j];
                    var jobDate = moment(jobLogs.startTime).format('YYYY-MM-DD');
                    var jobRuntime = moment(jobLogs.endTime).valueOf() - moment(jobLogs.startTime).valueOf();
                    var newDate = { date : jobDate, runtime : jobRuntime };

                    if (dataArray.length > 0) { // Dates have been added to dataArray
                        var containsDate = false;
                        for (var k = 0, kLen = dataArray.length; k < kLen; k++) {
                            if (dataArray[k].date === jobDate) { // dataArray already contains an entry for this date
                                dataArray[k].runtime += jobRuntime;
                                containsDate = true;
                                break;
                            }
                        }

                        if (!containsDate) { // dataArray did not contain an entry for this date
                            dataArray.push(newDate);
                        }
                    } else { // No dates have been added to dataArray, so add the first
                        dataArray.push(newDate);
                    }
                }
            }

            // Sort the series data by name (affects its order in the legend)
            var seriesData = dataArray.sort(function (a,b) { return a.date.localeCompare(b.date); });

            // Get the dates to be used as x axis categories and the series data
            var chartData = getTotalRuntimeSeriesData(seriesData);

            // Create the chart object
            return columnChartFormatting([{ data: chartData.seriesData }], chartData.dateRange, 3, false, 500, 700);
        }

        /* Run initial lookup */
        $scope.fetchJobs();

        /* Create charts */
        $scope.drop_runtime_chart = getDropRuntimesChart();
        $scope.total_runtime_chart = getTotalRuntimesChart();
    };
    
    return ['$scope', '$http', '$timeout', StatsCtrl];
});

