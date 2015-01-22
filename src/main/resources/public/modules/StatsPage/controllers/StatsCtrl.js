define([], function () {
    var StatsCtrl = function($scope, $http, $timeout) {

        $scope.jobLogsBeingViewed = []; // a list of jobs logs being viewed (to be 'reopened' after refresh)
        $scope.refreshInterval = 300000; // refresh time in milliseconds (5min)

        /** fetch all jobs and their logs */
        $scope.fetchJobs = function() {
            $scope.lastFetch = new Date();
            $http.get('/jobs')
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

        /* Add days to a date */
        Date.prototype.addDays = function(days) {
            var dat = new Date(this.valueOf());
            dat.setDate(dat.getDate() + days);
            return dat;
        };

        /* Get all dates between a start and end date */
        function getDateRange(startDate, stopDate) {
            var dateArray = [];
            var currentDate = startDate;
            while (currentDate <= stopDate) {
                dateArray.push(currentDate.getTime());
                currentDate = currentDate.addDays(1);
            }
            return dateArray;
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
                    formatter: function() {
                        return '<b>' + this.series.name + '</b><br/>' + Highcharts.dateFormat('%Y-%m-%d', this.x) + ': ' + this.y;
                    }
                },

                credits: {
                    enabled: false
                },

                series: seriesData
            };
        }

        /* Organise data into chart x axis categories */
        function getDropRuntimeSeriesData(seriesData) {
            var minDate = new Date();
            for (i = 0; i < seriesData.length; i++) {
                for (j = 0; j < seriesData[i]['data'].length; j++) {
                    if (new Date(seriesData[i]['data'][j][0]) < minDate) {
                        minDate = new Date(seriesData[i]['data'][j][0])
                    }
                }
            }

            // Get range of categories to be shown in graph
            var dateRange = getDateRange(minDate, new Date());

            // Assign each series point to a category ID
            for (i = 0; i < seriesData.length; i++) {
                for (j = 0; j < seriesData[i]['data'].length; j++) {
                    var index = -1;
                    for (k = 0; k < dateRange.length; k++) {
                        if (dateRange[k] == new Date(seriesData[i]['data'][j][0]).getTime()) {
                            index = k
                        }
                    }
                    seriesData[i]['data'][j][0] = index
                }
            }

            return {seriesData: seriesData, dateRange: dateRange}
        }

        /* Organise data into 1 value for each date */
        function getTotalRuntimeSeriesData(seriesData) {
            var minDate = new Date();
            if (seriesData.length > 0) {
                // seriesData is sorted, so minDate is first in array
                minDate = seriesData[0]['date'];
            } else {
                console.error("No logs present")
            }

            // Get range of categories to be shown in graph
            var dateRange = getDateRange(new Date(minDate), new Date());

            // Add empty entries for non-existant dates
            for (i = 0; i < dateRange.length; i++) {
                for (j = 0; j < seriesData.length; j++) {
                    var containsDate = false;
                    if (new Date(seriesData[j]['date']).getTime() == dateRange[i]) {
                        containsDate = true;
                        break;
                    }
                }
                if (!containsDate) {
                    seriesData.push({date: Highcharts.dateFormat('%Y-%m-%d', dateRange[i]), runtime: 0});
                }
            }

            // Reformat as a flat array, as there is only 1 series
            seriesData = seriesData.sort(function(a,b) { return a.date.localeCompare(b.date)});
            var outputArray = [];
            for (i = 0; i < seriesData.length; i++) {
                outputArray.push(seriesData[i]['runtime'])
            }

            return {seriesData: outputArray, dateRange: dateRange}
        }

        /* Get jobs data from API */
        function getDropRuntimesChart() {
            // Get all logs for all jobs, formatted to be accepted by Highcharts
            var dataArray = [];
            for (i = 0; i < $scope.jobCount; i++) {
                var job = {};
                job['name'] = $scope.jobs[i]['name'];
                job['data'] = [];
                for (j = 0; j < $scope.jobs[i]['logData'].length; j++) {
                    var logs = $scope.jobs[i]['logData'][j];
                    var start = new Date(logs['startTime']);
                    var end = new Date(logs['endTime']);
                    var x = Highcharts.dateFormat('%Y-%m-%d', start);
                    var y = end.getTime() - start.getTime();
                    job['data'].push([x, y]);
                }
                dataArray.push(job);
            }

            // Sort the series data by name (affects its order in the legend)
            var seriesData = dataArray.sort(function(a,b) { return a.name.localeCompare(b.name)});

            // Get the dates to be used as x axis categories and the series data
            var chartData = getDropRuntimeSeriesData(seriesData);

            // Create the chart object
            return columnChartFormatting(chartData['seriesData'], chartData['dateRange'], 1, true, 500, 1500);
        }

        /* Get jobs data from API */
        function getTotalRuntimesChart() {
            // Get all logs for all jobs, formatted to be accepted by Highcharts
            var dataArray = [];
            for (i = 0; i < $scope.jobCount; i++) {
                for (j = 0; j < $scope.jobs[i]['logData'].length; j++) {
                    var jobLogs = $scope.jobs[i]['logData'][j];
                    var jobDate = Highcharts.dateFormat('%Y-%m-%d', new Date(jobLogs['endTime']));
                    var jobRuntime = new Date(jobLogs['endTime']).getTime() - new Date(jobLogs['startTime']).getTime();
                    var newDate = { date : jobDate, runtime : jobRuntime };

                    if (dataArray.length > 0) { // Dates have been added to dataArray
                        var containsDate = false;
                        for (k = 0; k < dataArray.length; k++) {
                            if (dataArray[k]['date'] === jobDate) { // dataArray already contains an entry for this date
                                dataArray[k]['runtime'] += jobRuntime;
                                containsDate = true;
                                break;
                            }
                        }

                        if (!containsDate) { // dataArray did not contain an entry for this date
                            dataArray.push(newDate)
                        }
                    } else { // No dates have been added to dataArray, so add the first
                        dataArray.push(newDate)
                    }
                }
            }

            // Sort the series data by name (affects its order in the legend)
            var seriesData = dataArray.sort(function(a,b) { return a.date.localeCompare(b.date)});

            // Get the dates to be used as x axis categories and the series data
            var chartData = getTotalRuntimeSeriesData(seriesData);

            // Create the chart object
            return columnChartFormatting([{data: chartData['seriesData']}], chartData['dateRange'], 3, false, 500, 700);
        }

        /* Run initial lookup */
        $scope.fetchJobs();

        /* Create charts */
        $scope.drop_runtime_chart = getDropRuntimesChart();
        $scope.total_runtime_chart = getTotalRuntimesChart();
    };
    
    return ['$scope', '$http', '$timeout', StatsCtrl]
});
