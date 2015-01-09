define(['app'], function (app) {
    app.directive('highchart', function () {
        return {
            restrict: 'E',
            template: '<div></div>',
            replace: true,
            link: function (scope, element, attrs) {
                scope.$watch(function() { return attrs.chart; }, function() {
                    if(!attrs.chart) return;
                    var chart = JSON.parse(attrs.chart);
                    $(element[0]).highcharts(chart);
                });
            }
        }
    });

    app.controller('HistoryCtrl', ['$scope', '$http', '$timeout', function ($scope, $http, $timeout) {

        $scope.jobLogsBeingViewed = []; // a list of jobs logs being viewed (to be 'reopened' after refresh)
        $scope.refreshInterval = 300000; // refresh time in milliseconds (5min)

        var DisplayFilter = {
            SUCCESSES: 'successes',
            FAILURES: 'failures'
        };

        var testJobs = {
            "jobs": [
                {
                    "name": "AD-X Tracking",
                    "dropUID": "AdxTrackingToRedshiftDrop",
                    "description": "external.adx_daily_campaigns",
                    "enabled": true,
                    "cron": "0 0 1 * * ?",
                    "jobID": 1,
                    "configuration": {},
                    "timeFrame": "DAY_TWO_DAYS_AGO",
                    "parallel": false
                },
                {
                    "name": "Crosspromo Tool ELB Logs",
                    "dropUID": "AmazonServerAccessLogsToRedshiftDrop",
                    "description": "external.crosspromo_access",
                    "enabled": true,
                    "cron": "0 5 1 * * ?",
                    "jobID": 14,
                    "configuration": {},
                    "timeFrame": "DAY_YESTERDAY",
                    "parallel": false
                },
                {
                    "name": "Exchange Rates",
                    "dropUID": "ExchangeRatesToRedshiftDrop",
                    "description": "external.exchange_rates_l",
                    "enabled": true,
                    "cron": "0 5 1 * * ?",
                    "jobID": 2,
                    "configuration": {},
                    "timeFrame": "DAY_YESTERDAY",
                    "parallel": false
                },
                {
                    "name": "Moshling Rescue Facebook Insights",
                    "dropUID": "FacebookInsightsToRedshiftDrop",
                    "description": "external.facebook_insights",
                    "enabled": false,
                    "cron": "0 10 1 * * ?",
                    "jobID": 4,
                    "configuration": {
                        "configFilePath": "/facebook_insights/mr_facebook_insights.properties"
                    },
                    "timeFrame": "DAY_TWO_DAYS_AGO",
                    "parallel": false
                },
                {
                    "name": "Warriors Facebook Insights",
                    "dropUID": "FacebookInsightsToRedshiftDrop",
                    "description": "external.facebook_insights",
                    "enabled": false,
                    "cron": "0 15 1 * * ?",
                    "jobID": 24,
                    "configuration": {
                        "configFilePath": "/facebook_insights/w_facebook_insights.properties"
                    },
                    "timeFrame": "DAY_TWO_DAYS_AGO",
                    "parallel": false
                },
                {
                    "name": "Google Play Sales",
                    "dropUID": "GooglePlayToRedshiftDrop",
                    "description": "external.googleplay_daily_sales",
                    "enabled": false,
                    "cron": "0 55 5 * * ?",
                    "jobID": 23,
                    "configuration": {},
                    "timeFrame": "DAY_YESTERDAY",
                    "parallel": false
                },
                {
                    "name": "Itunes Daily Sales",
                    "dropUID": "ItunesSalesToRedshiftDrop",
                    "description": "external.itunes_daily_sales",
                    "enabled": true,
                    "cron": "0 0 6 * * ?",
                    "jobID": 3,
                    "configuration": {},
                    "timeFrame": "DAY_TWO_DAYS_AGO",
                    "parallel": false
                },
                {
                    "name": "Moshling Rescue Devices",
                    "dropUID": "PassThroughRedshiftAggregationDrop",
                    "description": "aggregates.moshlingrescue_devices_l",
                    "enabled": true,
                    "cron": "0 15 2 * * ?",
                    "jobID": 5,
                    "configuration": {
                        "configFilePath": "/aggregation/mr_devices.properties"
                    },
                    "timeFrame": "DAY_TODAY",
                    "parallel": false
                },
                {
                    "name": "Warriors IDs",
                    "dropUID": "PassThroughRedshiftAggregationDrop",
                    "description": "aggregates.warriors_ids_l",
                    "enabled": true,
                    "cron": "0 0 5 * * ?",
                    "jobID": 6,
                    "configuration": {
                        "configFilePath": "/aggregation/w_ids.properties"
                    },
                    "timeFrame": "DAY_TODAY",
                    "parallel": false
                }
            ],
            "count": 9
        };

        var testLogs = {
            "logs": [
                {
                    "startTime": "2014-12-23T01:00:00.022Z",
                    "exception": null,
                    "logOutput": null,
                    "endTime": "2014-12-23T01:13:21.291Z",
                    "runID": "5422b954-c289-4b4f-8de0-dd3f17bf8989",
                    "jobID": 1
                },
                {
                    "startTime": "2014-12-22T01:00:00.022Z",
                    "exception": null,
                    "logOutput": null,
                    "endTime": "2014-12-22T01:12:32.028Z",
                    "runID": "b8c5168c-1d57-4810-80fd-9424b9d052d8",
                    "jobID": 1
                },
                {
                    "startTime": "2014-12-21T01:00:00.023Z",
                    "exception": null,
                    "logOutput": null,
                    "endTime": "2014-12-21T01:00:24.582Z",
                    "runID": "5f81f062-fd13-4d35-8287-b371412688ac",
                    "jobID": 1
                },
                {
                    "startTime": "2014-12-20T01:00:00.022Z",
                    "exception": null,
                    "logOutput": null,
                    "endTime": "2014-12-20T01:24:21.327Z",
                    "runID": "421fce34-5f33-4f47-a8e8-14249bff8ff9",
                    "jobID": 1
                },
                {
                    "startTime": "2014-12-19T01:00:00.022Z",
                    "exception": null,
                    "logOutput": null,
                    "endTime": "2014-12-19T01:01:01.355Z",
                    "runID": "4b271896-1f59-45d7-85b3-253d0cec073d",
                    "jobID": 1
                },
                {
                    "startTime": "2014-12-18T01:00:00.026Z",
                    "exception": null,
                    "logOutput": null,
                    "endTime": "2014-12-18T01:25:53.177Z",
                    "runID": "15f30959-7ae3-450e-b3ba-e129eff1d825",
                    "jobID": 1
                },
                {
                    "startTime": "2014-12-17T10:00:39.678Z",
                    "exception": "org.postgresql.util.PSQLException: ERROR: The specified S3 prefix 'adx_daily_campaigns-20141215' does not exist\n  Detail: \n  -----------------------------------------------\n  error:  The specified S3 prefix 'adx_daily_campaigns-20141215' does not exist\n  code:      8001\n  context:   \n  query:     337082\n  location:  s3_utility.cpp:539\n  process:   padbmaster [pid=1320]\n  -----------------------------------------------\n\norg.postgresql.core.v3.QueryExecutorImpl.receiveErrorResponse(QueryExecutorImpl.java:2102)\norg.postgresql.core.v3.QueryExecutorImpl.processResults(QueryExecutorImpl.java:1835)\norg.postgresql.core.v3.QueryExecutorImpl.execute(QueryExecutorImpl.java:257)\norg.postgresql.jdbc2.AbstractJdbc2Statement.execute(AbstractJdbc2Statement.java:500)\norg.postgresql.jdbc2.AbstractJdbc2Statement.executeWithFlags(AbstractJdbc2Statement.java:388)\norg.postgresql.jdbc2.AbstractJdbc2Statement.execute(AbstractJdbc2Statement.java:381)\nscala.slick.jdbc.StatementInvoker.results(StatementInvoker.scala:38)\nscala.slick.jdbc.StatementInvoker.iteratorTo(StatementInvoker.scala:22)\nscala.slick.jdbc.Invoker$class.iterator(Invoker.scala:15)\nscala.slick.jdbc.StatementInvoker.iterator(StatementInvoker.scala:16)\nscala.slick.jdbc.Invoker$class.execute(Invoker.scala:23)\nscala.slick.jdbc.StatementInvoker.execute(StatementInvoker.scala:16)\ncom.mindcandy.waterfall.io.RedshiftIOSink$$anonfun$storeFrom$1$$anonfun$apply$mcV$sp$2.apply$mcV$sp(RedshiftIO.scala:64)\ncom.mindcandy.waterfall.io.RedshiftIOSink$$anonfun$storeFrom$1$$anonfun$apply$mcV$sp$2.apply(RedshiftIO.scala:56)\ncom.mindcandy.waterfall.io.RedshiftIOSink$$anonfun$storeFrom$1$$anonfun$apply$mcV$sp$2.apply(RedshiftIO.scala:56)\nscala.util.DynamicVariable.withValue(DynamicVariable.scala:57)\nscala.slick.backend.DatabaseComponent$class.withDynamicSession(DatabaseComponent.scala:67)\nscala.slick.jdbc.JdbcBackend$.withDynamicSession(JdbcBackend.scala:448)\nscala.slick.backend.DatabaseComponent$DatabaseDef$$anonfun$withDynSession$1.apply(DatabaseComponent.scala:51)\nscala.slick.backend.DatabaseComponent$DatabaseDef$$anonfun$withDynSession$1.apply(DatabaseComponent.scala:51)\nscala.slick.backend.DatabaseComponent$DatabaseDef$class.withSession(DatabaseComponent.scala:34)\nscala.slick.jdbc.JdbcBackend$DatabaseFactoryDef$$anon$4.withSession(JdbcBackend.scala:61)\nscala.slick.backend.DatabaseComponent$DatabaseDef$class.withDynSession(DatabaseComponent.scala:51)\nscala.slick.jdbc.JdbcBackend$DatabaseFactoryDef$$anon$4.withDynSession(JdbcBackend.scala:61)\ncom.mindcandy.waterfall.io.RedshiftIOSink$$anonfun$storeFrom$1.apply$mcV$sp(RedshiftIO.scala:56)\ncom.mindcandy.waterfall.io.RedshiftIOSink$$anonfun$storeFrom$1.apply(RedshiftIO.scala:56)\ncom.mindcandy.waterfall.io.RedshiftIOSink$$anonfun$storeFrom$1.apply(RedshiftIO.scala:56)\nscala.util.Try$.apply(Try.scala:161)\ncom.mindcandy.waterfall.io.RedshiftIOSink.storeFrom(RedshiftIO.scala:56)\ncom.mindcandy.waterfall.WaterfallDrop$$anonfun$run$1$$anonfun$apply$1.apply(WaterfallDrop.scala:23)\ncom.mindcandy.waterfall.WaterfallDrop$$anonfun$run$1$$anonfun$apply$1.apply(WaterfallDrop.scala:22)\nscala.util.Success.flatMap(Try.scala:200)\ncom.mindcandy.waterfall.WaterfallDrop$$anonfun$run$1.apply(WaterfallDrop.scala:22)\ncom.mindcandy.waterfall.WaterfallDrop$$anonfun$run$1.apply(WaterfallDrop.scala:21)\nscala.util.Success.flatMap(Try.scala:200)\ncom.mindcandy.waterfall.WaterfallDrop$class.run(WaterfallDrop.scala:21)\ncom.mindcandy.waterfall.drop.AdxTrackingToRedshiftDrop.run(AdxTrackingToRedshiftDrop.scala:65)\ncom.mindcandy.waterfall.actor.DropWorker$$anonfun$receive$1.applyOrElse(DropWorker.scala:19)\nakka.actor.Actor$class.aroundReceive(Actor.scala:465)\ncom.mindcandy.waterfall.actor.DropWorker.aroundReceive(DropWorker.scala:14)\nakka.actor.ActorCell.receiveMessage(ActorCell.scala:516)\nakka.actor.ActorCell.invoke(ActorCell.scala:487)\nakka.dispatch.Mailbox.processMailbox(Mailbox.scala:238)\nakka.dispatch.Mailbox.run(Mailbox.scala:220)\nakka.dispatch.ForkJoinExecutorConfigurator$AkkaForkJoinTask.exec(AbstractDispatcher.scala:393)\nscala.concurrent.forkjoin.ForkJoinTask.doExec(ForkJoinTask.java:260)\nscala.concurrent.forkjoin.ForkJoinPool$WorkQueue.runTask(ForkJoinPool.java:1339)\nscala.concurrent.forkjoin.ForkJoinPool.runWorker(ForkJoinPool.java:1979)\nscala.concurrent.forkjoin.ForkJoinWorkerThread.run(ForkJoinWorkerThread.java:107)\n",
                    "logOutput": null,
                    "endTime": "2014-12-17T10:01:21.739Z",
                    "runID": "90934378-b22e-4306-908a-ae2ef860f5ce",
                    "jobID": 1
                },
                {
                    "startTime": "2014-12-23T01:05:00.022Z",
                    "exception": null,
                    "logOutput": null,
                    "endTime": "2014-12-23T01:15:20.735Z",
                    "runID": "7e1cbcdd-318d-4100-8761-8fe2d7e9c6e2",
                    "jobID": 14
                },
                {
                    "startTime": "2014-12-22T01:05:00.012Z",
                    "exception": null,
                    "logOutput": null,
                    "endTime": "2014-12-22T01:13:22.056Z",
                    "runID": "3b1a5505-e1ce-4925-b640-2717ac0d5975",
                    "jobID": 14
                },
                {
                    "startTime": "2014-12-21T01:05:00.022Z",
                    "exception": null,
                    "logOutput": null,
                    "endTime": "2014-12-21T01:06:16.366Z",
                    "runID": "dc330049-e319-42dd-8461-2e975ee2f33e",
                    "jobID": 14
                },
                {
                    "startTime": "2014-12-20T01:05:00.012Z",
                    "exception": null,
                    "logOutput": null,
                    "endTime": "2014-12-20T01:24:26.594Z",
                    "runID": "1bc6017d-7863-401d-9687-e2fe8c588bfc",
                    "jobID": 14
                },
                {
                    "startTime": "2014-12-19T01:05:00.084Z",
                    "exception": null,
                    "logOutput": null,
                    "endTime": "2014-12-19T01:07:34.077Z",
                    "runID": "cd145442-c607-4206-b897-e85914c8fcdb",
                    "jobID": 14
                },
                {
                    "startTime": "2014-12-18T01:05:00.024Z",
                    "exception": null,
                    "logOutput": null,
                    "endTime": "2014-12-18T01:26:09.120Z",
                    "runID": "6ba497e9-719a-4a10-8c52-2c7f052bea5d",
                    "jobID": 14
                },
                {
                    "startTime": "2014-12-17T01:05:00.029Z",
                    "exception": null,
                    "logOutput": null,
                    "endTime": "2014-12-17T01:06:54.372Z",
                    "runID": "6473b29d-4140-4313-bafc-3d5db08cd6f2",
                    "jobID": 14
                },
                {
"startTime": "2014-12-23T01:05:00.030Z",
"exception": null,
"logOutput": null,
"endTime": "2014-12-23T01:15:20.663Z",
"runID": "c831979b-681f-45ed-ba6b-24cad749963f",
"jobID": 2
},
{
"startTime": "2014-12-22T01:05:00.077Z",
"exception": null,
"logOutput": null,
"endTime": "2014-12-22T01:12:48.324Z",
"runID": "348b50b4-b1eb-4e7d-ac71-721030e1f5fe",
"jobID": 2
},
{
"startTime": "2014-12-21T01:05:00.023Z",
"exception": null,
"logOutput": null,
"endTime": "2014-12-21T01:05:25.875Z",
"runID": "c3a3d165-7ff1-4fac-8207-d7552c57a771",
"jobID": 2
},
{
"startTime": "2014-12-20T01:05:00.022Z",
"exception": null,
"logOutput": null,
"endTime": "2014-12-20T01:24:26.536Z",
"runID": "ac243ee8-8115-4619-b7e4-c6c7987e4592",
"jobID": 2
},
{
"startTime": "2014-12-19T01:05:00.100Z",
"exception": null,
"logOutput": null,
"endTime": "2014-12-19T01:05:19.552Z",
"runID": "ed34dc57-f6a6-49f5-96d5-9bca35c6e241",
"jobID": 2
},
{
"startTime": "2014-12-18T01:05:00.015Z",
"exception": null,
"logOutput": null,
"endTime": "2014-12-18T01:26:05.682Z",
"runID": "fdc2862f-48b3-457b-b161-2279102bc408",
"jobID": 2
},
{
"startTime": "2014-12-17T09:58:28.024Z",
"exception": null,
"logOutput": null,
"endTime": "2014-12-17T09:59:06.761Z",
"runID": "bf3c46a1-8df5-4f68-b7a7-9ad5f4b1edc4",
"jobID": 2
},
{
"startTime": "2014-11-21T01:10:00.016Z",
"exception": "org.postgresql.util.PSQLException: ERROR: The specified S3 prefix 'moshling_rescue-20141119' does not exist\ Detail: \ -----------------------------------------------\ error: The specified S3 prefix 'moshling_rescue-20141119' does not exist\ code: 8001\ context: \ query: 175862\ location: s3_utility.cpp:539\ process: padbmaster [pid=22789]\ -----------------------------------------------\ \ org.postgresql.core.v3.QueryExecutorImpl.receiveErrorResponse(QueryExecutorImpl.java:2102)\ org.postgresql.core.v3.QueryExecutorImpl.processResults(QueryExecutorImpl.java:1835)\ org.postgresql.core.v3.QueryExecutorImpl.execute(QueryExecutorImpl.java:257)\ org.postgresql.jdbc2.AbstractJdbc2Statement.execute(AbstractJdbc2Statement.java:500)\ org.postgresql.jdbc2.AbstractJdbc2Statement.executeWithFlags(AbstractJdbc2Statement.java:388)\ org.postgresql.jdbc2.AbstractJdbc2Statement.execute(AbstractJdbc2Statement.java:381)\ scala.slick.jdbc.StatementInvoker.results(StatementInvoker.scala:38)\ scala.slick.jdbc.StatementInvoker.iteratorTo(StatementInvoker.scala:22)\ scala.slick.jdbc.Invoker$class.iterator(Invoker.scala:15)\ scala.slick.jdbc.StatementInvoker.iterator(StatementInvoker.scala:16)\ scala.slick.jdbc.Invoker$class.execute(Invoker.scala:23)\ scala.slick.jdbc.StatementInvoker.execute(StatementInvoker.scala:16)\ com.mindcandy.waterfall.io.RedshiftIOSink$$anonfun$storeFrom$1$$anonfun$apply$mcV$sp$2.apply$mcV$sp(RedshiftIO.scala:64)\ com.mindcandy.waterfall.io.RedshiftIOSink$$anonfun$storeFrom$1$$anonfun$apply$mcV$sp$2.apply(RedshiftIO.scala:56)\ com.mindcandy.waterfall.io.RedshiftIOSink$$anonfun$storeFrom$1$$anonfun$apply$mcV$sp$2.apply(RedshiftIO.scala:56)\ scala.util.DynamicVariable.withValue(DynamicVariable.scala:57)\ scala.slick.backend.DatabaseComponent$class.withDynamicSession(DatabaseComponent.scala:67)\ scala.slick.jdbc.JdbcBackend$.withDynamicSession(JdbcBackend.scala:448)\ scala.slick.backend.DatabaseComponent$DatabaseDef$$anonfun$withDynSession$1.apply(DatabaseComponent.scala:51)\ scala.slick.backend.DatabaseComponent$DatabaseDef$$anonfun$withDynSession$1.apply(DatabaseComponent.scala:51)\ scala.slick.backend.DatabaseComponent$DatabaseDef$class.withSession(DatabaseComponent.scala:34)\ scala.slick.jdbc.JdbcBackend$DatabaseFactoryDef$$anon$4.withSession(JdbcBackend.scala:61)\ scala.slick.backend.DatabaseComponent$DatabaseDef$class.withDynSession(DatabaseComponent.scala:51)\ scala.slick.jdbc.JdbcBackend$DatabaseFactoryDef$$anon$4.withDynSession(JdbcBackend.scala:61)\ com.mindcandy.waterfall.io.RedshiftIOSink$$anonfun$storeFrom$1.apply$mcV$sp(RedshiftIO.scala:56)\ com.mindcandy.waterfall.io.RedshiftIOSink$$anonfun$storeFrom$1.apply(RedshiftIO.scala:56)\ com.mindcandy.waterfall.io.RedshiftIOSink$$anonfun$storeFrom$1.apply(RedshiftIO.scala:56)\ scala.util.Try$.apply(Try.scala:161)\ com.mindcandy.waterfall.io.RedshiftIOSink.storeFrom(RedshiftIO.scala:56)\ com.mindcandy.waterfall.WaterfallDrop$$anonfun$run$1$$anonfun$apply$1.apply(WaterfallDrop.scala:23)\ com.mindcandy.waterfall.WaterfallDrop$$anonfun$run$1$$anonfun$apply$1.apply(WaterfallDrop.scala:22)\ scala.util.Success.flatMap(Try.scala:200)\ com.mindcandy.waterfall.WaterfallDrop$$anonfun$run$1.apply(WaterfallDrop.scala:22)\ com.mindcandy.waterfall.WaterfallDrop$$anonfun$run$1.apply(WaterfallDrop.scala:21)\ scala.util.Success.flatMap(Try.scala:200)\ com.mindcandy.waterfall.WaterfallDrop$class.run(WaterfallDrop.scala:21)\ com.mindcandy.waterfall.drop.FacebookInsightsToRedshiftDrop.run(FacebookInsightsToRedshiftDrop.scala:46)\ com.mindcandy.waterfall.actor.DropWorker$$anonfun$receive$1.applyOrElse(DropWorker.scala:19)\ akka.actor.Actor$class.aroundReceive(Actor.scala:465)\ com.mindcandy.waterfall.actor.DropWorker.aroundReceive(DropWorker.scala:14)\ akka.actor.ActorCell.receiveMessage(ActorCell.scala:516)\ akka.actor.ActorCell.invoke(ActorCell.scala:487)\ akka.dispatch.Mailbox.processMailbox(Mailbox.scala:238)\ akka.dispatch.Mailbox.run(Mailbox.scala:220)\ akka.dispatch.ForkJoinExecutorConfigurator$AkkaForkJoinTask.exec(AbstractDispatcher.scala:393)\ scala.concurrent.forkjoin.ForkJoinTask.doExec(ForkJoinTask.java:260)\ scala.concurrent.forkjoin.ForkJoinPool$WorkQueue.runTask(ForkJoinPool.java:1339)\ scala.concurrent.forkjoin.ForkJoinPool.runWorker(ForkJoinPool.java:1979)\ scala.concurrent.forkjoin.ForkJoinWorkerThread.run(ForkJoinWorkerThread.java:107)\ ",
"logOutput": null,
"endTime": "2014-11-21T01:10:35.805Z",
"runID": "93f4a34f-a81e-4f82-8028-db73ecfc8191",
"jobID": 4
},
{
"startTime": "2014-11-20T01:10:00.017Z",
"exception": "org.postgresql.util.PSQLException: ERROR: The specified S3 prefix 'moshling_rescue-20141118' does not exist\ Detail: \ -----------------------------------------------\ error: The specified S3 prefix 'moshling_rescue-20141118' does not exist\ code: 8001\ context: \ query: 155442\ location: s3_utility.cpp:539\ process: padbmaster [pid=32300]\ -----------------------------------------------\ \ org.postgresql.core.v3.QueryExecutorImpl.receiveErrorResponse(QueryExecutorImpl.java:2102)\ org.postgresql.core.v3.QueryExecutorImpl.processResults(QueryExecutorImpl.java:1835)\ org.postgresql.core.v3.QueryExecutorImpl.execute(QueryExecutorImpl.java:257)\ org.postgresql.jdbc2.AbstractJdbc2Statement.execute(AbstractJdbc2Statement.java:500)\ org.postgresql.jdbc2.AbstractJdbc2Statement.executeWithFlags(AbstractJdbc2Statement.java:388)\ org.postgresql.jdbc2.AbstractJdbc2Statement.execute(AbstractJdbc2Statement.java:381)\ scala.slick.jdbc.StatementInvoker.results(StatementInvoker.scala:38)\ scala.slick.jdbc.StatementInvoker.iteratorTo(StatementInvoker.scala:22)\ scala.slick.jdbc.Invoker$class.iterator(Invoker.scala:15)\ scala.slick.jdbc.StatementInvoker.iterator(StatementInvoker.scala:16)\ scala.slick.jdbc.Invoker$class.execute(Invoker.scala:23)\ scala.slick.jdbc.StatementInvoker.execute(StatementInvoker.scala:16)\ com.mindcandy.waterfall.io.RedshiftIOSink$$anonfun$storeFrom$1$$anonfun$apply$mcV$sp$2.apply$mcV$sp(RedshiftIO.scala:64)\ com.mindcandy.waterfall.io.RedshiftIOSink$$anonfun$storeFrom$1$$anonfun$apply$mcV$sp$2.apply(RedshiftIO.scala:56)\ com.mindcandy.waterfall.io.RedshiftIOSink$$anonfun$storeFrom$1$$anonfun$apply$mcV$sp$2.apply(RedshiftIO.scala:56)\ scala.util.DynamicVariable.withValue(DynamicVariable.scala:57)\ scala.slick.backend.DatabaseComponent$class.withDynamicSession(DatabaseComponent.scala:67)\ scala.slick.jdbc.JdbcBackend$.withDynamicSession(JdbcBackend.scala:448)\ scala.slick.backend.DatabaseComponent$DatabaseDef$$anonfun$withDynSession$1.apply(DatabaseComponent.scala:51)\ scala.slick.backend.DatabaseComponent$DatabaseDef$$anonfun$withDynSession$1.apply(DatabaseComponent.scala:51)\ scala.slick.backend.DatabaseComponent$DatabaseDef$class.withSession(DatabaseComponent.scala:34)\ scala.slick.jdbc.JdbcBackend$DatabaseFactoryDef$$anon$4.withSession(JdbcBackend.scala:61)\ scala.slick.backend.DatabaseComponent$DatabaseDef$class.withDynSession(DatabaseComponent.scala:51)\ scala.slick.jdbc.JdbcBackend$DatabaseFactoryDef$$anon$4.withDynSession(JdbcBackend.scala:61)\ com.mindcandy.waterfall.io.RedshiftIOSink$$anonfun$storeFrom$1.apply$mcV$sp(RedshiftIO.scala:56)\ com.mindcandy.waterfall.io.RedshiftIOSink$$anonfun$storeFrom$1.apply(RedshiftIO.scala:56)\ com.mindcandy.waterfall.io.RedshiftIOSink$$anonfun$storeFrom$1.apply(RedshiftIO.scala:56)\ scala.util.Try$.apply(Try.scala:161)\ com.mindcandy.waterfall.io.RedshiftIOSink.storeFrom(RedshiftIO.scala:56)\ com.mindcandy.waterfall.WaterfallDrop$$anonfun$run$1$$anonfun$apply$1.apply(WaterfallDrop.scala:23)\ com.mindcandy.waterfall.WaterfallDrop$$anonfun$run$1$$anonfun$apply$1.apply(WaterfallDrop.scala:22)\ scala.util.Success.flatMap(Try.scala:200)\ com.mindcandy.waterfall.WaterfallDrop$$anonfun$run$1.apply(WaterfallDrop.scala:22)\ com.mindcandy.waterfall.WaterfallDrop$$anonfun$run$1.apply(WaterfallDrop.scala:21)\ scala.util.Success.flatMap(Try.scala:200)\ com.mindcandy.waterfall.WaterfallDrop$class.run(WaterfallDrop.scala:21)\ com.mindcandy.waterfall.drop.FacebookInsightsToRedshiftDrop.run(FacebookInsightsToRedshiftDrop.scala:46)\ com.mindcandy.waterfall.actor.DropWorker$$anonfun$receive$1.applyOrElse(DropWorker.scala:19)\ akka.actor.Actor$class.aroundReceive(Actor.scala:465)\ com.mindcandy.waterfall.actor.DropWorker.aroundReceive(DropWorker.scala:14)\ akka.actor.ActorCell.receiveMessage(ActorCell.scala:516)\ akka.actor.ActorCell.invoke(ActorCell.scala:487)\ akka.dispatch.Mailbox.processMailbox(Mailbox.scala:238)\ akka.dispatch.Mailbox.run(Mailbox.scala:220)\ akka.dispatch.ForkJoinExecutorConfigurator$AkkaForkJoinTask.exec(AbstractDispatcher.scala:393)\ scala.concurrent.forkjoin.ForkJoinTask.doExec(ForkJoinTask.java:260)\ scala.concurrent.forkjoin.ForkJoinPool$WorkQueue.runTask(ForkJoinPool.java:1339)\ scala.concurrent.forkjoin.ForkJoinPool.runWorker(ForkJoinPool.java:1979)\ scala.concurrent.forkjoin.ForkJoinWorkerThread.run(ForkJoinWorkerThread.java:107)\ ",
"logOutput": null,
"endTime": "2014-11-20T01:10:46.190Z",
"runID": "c3fd9fbe-375c-4e51-ba04-500c45bcec76",
"jobID": 4
},
{
"startTime": "2014-11-19T01:10:00.016Z",
"exception": "org.postgresql.util.PSQLException: ERROR: The specified S3 prefix 'moshling_rescue-20141117' does not exist\ Detail: \ -----------------------------------------------\ error: The specified S3 prefix 'moshling_rescue-20141117' does not exist\ code: 8001\ context: \ query: 136306\ location: s3_utility.cpp:539\ process: padbmaster [pid=2428]\ -----------------------------------------------\ \ org.postgresql.core.v3.QueryExecutorImpl.receiveErrorResponse(QueryExecutorImpl.java:2102)\ org.postgresql.core.v3.QueryExecutorImpl.processResults(QueryExecutorImpl.java:1835)\ org.postgresql.core.v3.QueryExecutorImpl.execute(QueryExecutorImpl.java:257)\ org.postgresql.jdbc2.AbstractJdbc2Statement.execute(AbstractJdbc2Statement.java:500)\ org.postgresql.jdbc2.AbstractJdbc2Statement.executeWithFlags(AbstractJdbc2Statement.java:388)\ org.postgresql.jdbc2.AbstractJdbc2Statement.execute(AbstractJdbc2Statement.java:381)\ scala.slick.jdbc.StatementInvoker.results(StatementInvoker.scala:38)\ scala.slick.jdbc.StatementInvoker.iteratorTo(StatementInvoker.scala:22)\ scala.slick.jdbc.Invoker$class.iterator(Invoker.scala:15)\ scala.slick.jdbc.StatementInvoker.iterator(StatementInvoker.scala:16)\ scala.slick.jdbc.Invoker$class.execute(Invoker.scala:23)\ scala.slick.jdbc.StatementInvoker.execute(StatementInvoker.scala:16)\ com.mindcandy.waterfall.io.RedshiftIOSink$$anonfun$storeFrom$1$$anonfun$apply$mcV$sp$2.apply$mcV$sp(RedshiftIO.scala:64)\ com.mindcandy.waterfall.io.RedshiftIOSink$$anonfun$storeFrom$1$$anonfun$apply$mcV$sp$2.apply(RedshiftIO.scala:56)\ com.mindcandy.waterfall.io.RedshiftIOSink$$anonfun$storeFrom$1$$anonfun$apply$mcV$sp$2.apply(RedshiftIO.scala:56)\ scala.util.DynamicVariable.withValue(DynamicVariable.scala:57)\ scala.slick.backend.DatabaseComponent$class.withDynamicSession(DatabaseComponent.scala:67)\ scala.slick.jdbc.JdbcBackend$.withDynamicSession(JdbcBackend.scala:448)\ scala.slick.backend.DatabaseComponent$DatabaseDef$$anonfun$withDynSession$1.apply(DatabaseComponent.scala:51)\ scala.slick.backend.DatabaseComponent$DatabaseDef$$anonfun$withDynSession$1.apply(DatabaseComponent.scala:51)\ scala.slick.backend.DatabaseComponent$DatabaseDef$class.withSession(DatabaseComponent.scala:34)\ scala.slick.jdbc.JdbcBackend$DatabaseFactoryDef$$anon$4.withSession(JdbcBackend.scala:61)\ scala.slick.backend.DatabaseComponent$DatabaseDef$class.withDynSession(DatabaseComponent.scala:51)\ scala.slick.jdbc.JdbcBackend$DatabaseFactoryDef$$anon$4.withDynSession(JdbcBackend.scala:61)\ com.mindcandy.waterfall.io.RedshiftIOSink$$anonfun$storeFrom$1.apply$mcV$sp(RedshiftIO.scala:56)\ com.mindcandy.waterfall.io.RedshiftIOSink$$anonfun$storeFrom$1.apply(RedshiftIO.scala:56)\ com.mindcandy.waterfall.io.RedshiftIOSink$$anonfun$storeFrom$1.apply(RedshiftIO.scala:56)\ scala.util.Try$.apply(Try.scala:161)\ com.mindcandy.waterfall.io.RedshiftIOSink.storeFrom(RedshiftIO.scala:56)\ com.mindcandy.waterfall.WaterfallDrop$$anonfun$run$1$$anonfun$apply$1.apply(WaterfallDrop.scala:23)\ com.mindcandy.waterfall.WaterfallDrop$$anonfun$run$1$$anonfun$apply$1.apply(WaterfallDrop.scala:22)\ scala.util.Success.flatMap(Try.scala:200)\ com.mindcandy.waterfall.WaterfallDrop$$anonfun$run$1.apply(WaterfallDrop.scala:22)\ com.mindcandy.waterfall.WaterfallDrop$$anonfun$run$1.apply(WaterfallDrop.scala:21)\ scala.util.Success.flatMap(Try.scala:200)\ com.mindcandy.waterfall.WaterfallDrop$class.run(WaterfallDrop.scala:21)\ com.mindcandy.waterfall.drop.FacebookInsightsToRedshiftDrop.run(FacebookInsightsToRedshiftDrop.scala:46)\ com.mindcandy.waterfall.actor.DropWorker$$anonfun$receive$1.applyOrElse(DropWorker.scala:19)\ akka.actor.Actor$class.aroundReceive(Actor.scala:465)\ com.mindcandy.waterfall.actor.DropWorker.aroundReceive(DropWorker.scala:14)\ akka.actor.ActorCell.receiveMessage(ActorCell.scala:516)\ akka.actor.ActorCell.invoke(ActorCell.scala:487)\ akka.dispatch.Mailbox.processMailbox(Mailbox.scala:238)\ akka.dispatch.Mailbox.run(Mailbox.scala:220)\ akka.dispatch.ForkJoinExecutorConfigurator$AkkaForkJoinTask.exec(AbstractDispatcher.scala:393)\ scala.concurrent.forkjoin.ForkJoinTask.doExec(ForkJoinTask.java:260)\ scala.concurrent.forkjoin.ForkJoinPool$WorkQueue.runTask(ForkJoinPool.java:1339)\ scala.concurrent.forkjoin.ForkJoinPool.runWorker(ForkJoinPool.java:1979)\ scala.concurrent.forkjoin.ForkJoinWorkerThread.run(ForkJoinWorkerThread.java:107)\ ",
"logOutput": null,
"endTime": "2014-11-19T01:10:16.833Z",
"runID": "9af7b489-5a0d-4370-a3bd-70a1438df117",
"jobID": 4
},
{
"startTime": "2014-11-18T10:48:33.990Z",
"exception": "org.postgresql.util.PSQLException: ERROR: The specified S3 prefix 'moshling_rescue-20141116' does not exist\ Detail: \ -----------------------------------------------\ error: The specified S3 prefix 'moshling_rescue-20141116' does not exist\ code: 8001\ context: \ query: 125717\ location: s3_utility.cpp:539\ process: padbmaster [pid=996]\ -----------------------------------------------\ \ org.postgresql.core.v3.QueryExecutorImpl.receiveErrorResponse(QueryExecutorImpl.java:2102)\ org.postgresql.core.v3.QueryExecutorImpl.processResults(QueryExecutorImpl.java:1835)\ org.postgresql.core.v3.QueryExecutorImpl.execute(QueryExecutorImpl.java:257)\ org.postgresql.jdbc2.AbstractJdbc2Statement.execute(AbstractJdbc2Statement.java:500)\ org.postgresql.jdbc2.AbstractJdbc2Statement.executeWithFlags(AbstractJdbc2Statement.java:388)\ org.postgresql.jdbc2.AbstractJdbc2Statement.execute(AbstractJdbc2Statement.java:381)\ scala.slick.jdbc.StatementInvoker.results(StatementInvoker.scala:38)\ scala.slick.jdbc.StatementInvoker.iteratorTo(StatementInvoker.scala:22)\ scala.slick.jdbc.Invoker$class.iterator(Invoker.scala:15)\ scala.slick.jdbc.StatementInvoker.iterator(StatementInvoker.scala:16)\ scala.slick.jdbc.Invoker$class.execute(Invoker.scala:23)\ scala.slick.jdbc.StatementInvoker.execute(StatementInvoker.scala:16)\ com.mindcandy.waterfall.io.RedshiftIOSink$$anonfun$storeFrom$1$$anonfun$apply$mcV$sp$2.apply$mcV$sp(RedshiftIO.scala:64)\ com.mindcandy.waterfall.io.RedshiftIOSink$$anonfun$storeFrom$1$$anonfun$apply$mcV$sp$2.apply(RedshiftIO.scala:56)\ com.mindcandy.waterfall.io.RedshiftIOSink$$anonfun$storeFrom$1$$anonfun$apply$mcV$sp$2.apply(RedshiftIO.scala:56)\ scala.util.DynamicVariable.withValue(DynamicVariable.scala:57)\ scala.slick.backend.DatabaseComponent$class.withDynamicSession(DatabaseComponent.scala:67)\ scala.slick.jdbc.JdbcBackend$.withDynamicSession(JdbcBackend.scala:448)\ scala.slick.backend.DatabaseComponent$DatabaseDef$$anonfun$withDynSession$1.apply(DatabaseComponent.scala:51)\ scala.slick.backend.DatabaseComponent$DatabaseDef$$anonfun$withDynSession$1.apply(DatabaseComponent.scala:51)\ scala.slick.backend.DatabaseComponent$DatabaseDef$class.withSession(DatabaseComponent.scala:34)\ scala.slick.jdbc.JdbcBackend$DatabaseFactoryDef$$anon$4.withSession(JdbcBackend.scala:61)\ scala.slick.backend.DatabaseComponent$DatabaseDef$class.withDynSession(DatabaseComponent.scala:51)\ scala.slick.jdbc.JdbcBackend$DatabaseFactoryDef$$anon$4.withDynSession(JdbcBackend.scala:61)\ com.mindcandy.waterfall.io.RedshiftIOSink$$anonfun$storeFrom$1.apply$mcV$sp(RedshiftIO.scala:56)\ com.mindcandy.waterfall.io.RedshiftIOSink$$anonfun$storeFrom$1.apply(RedshiftIO.scala:56)\ com.mindcandy.waterfall.io.RedshiftIOSink$$anonfun$storeFrom$1.apply(RedshiftIO.scala:56)\ scala.util.Try$.apply(Try.scala:161)\ com.mindcandy.waterfall.io.RedshiftIOSink.storeFrom(RedshiftIO.scala:56)\ com.mindcandy.waterfall.WaterfallDrop$$anonfun$run$1$$anonfun$apply$1.apply(WaterfallDrop.scala:23)\ com.mindcandy.waterfall.WaterfallDrop$$anonfun$run$1$$anonfun$apply$1.apply(WaterfallDrop.scala:22)\ scala.util.Success.flatMap(Try.scala:200)\ com.mindcandy.waterfall.WaterfallDrop$$anonfun$run$1.apply(WaterfallDrop.scala:22)\ com.mindcandy.waterfall.WaterfallDrop$$anonfun$run$1.apply(WaterfallDrop.scala:21)\ scala.util.Success.flatMap(Try.scala:200)\ com.mindcandy.waterfall.WaterfallDrop$class.run(WaterfallDrop.scala:21)\ com.mindcandy.waterfall.drop.FacebookInsightsToRedshiftDrop.run(FacebookInsightsToRedshiftDrop.scala:46)\ com.mindcandy.waterfall.actor.DropWorker$$anonfun$receive$1.applyOrElse(DropWorker.scala:19)\ akka.actor.Actor$class.aroundReceive(Actor.scala:465)\ com.mindcandy.waterfall.actor.DropWorker.aroundReceive(DropWorker.scala:14)\ akka.actor.ActorCell.receiveMessage(ActorCell.scala:516)\ akka.actor.ActorCell.invoke(ActorCell.scala:487)\ akka.dispatch.Mailbox.processMailbox(Mailbox.scala:238)\ akka.dispatch.Mailbox.run(Mailbox.scala:220)\ akka.dispatch.ForkJoinExecutorConfigurator$AkkaForkJoinTask.exec(AbstractDispatcher.scala:393)\ scala.concurrent.forkjoin.ForkJoinTask.doExec(ForkJoinTask.java:260)\ scala.concurrent.forkjoin.ForkJoinPool$WorkQueue.runTask(ForkJoinPool.java:1339)\ scala.concurrent.forkjoin.ForkJoinPool.runWorker(ForkJoinPool.java:1979)\ scala.concurrent.forkjoin.ForkJoinWorkerThread.run(ForkJoinWorkerThread.java:107)\ ",
"logOutput": null,
"endTime": "2014-11-18T10:49:51.298Z",
"runID": "8ee310c7-24fa-4751-8187-ac666215bd97",
"jobID": 4
},
{
"startTime": "2014-11-18T01:10:00.016Z",
"exception": "org.postgresql.util.PSQLException: ERROR: The specified S3 prefix 'moshling_rescue-20141116' does not exist\ Detail: \ -----------------------------------------------\ error: The specified S3 prefix 'moshling_rescue-20141116' does not exist\ code: 8001\ context: \ query: 117529\ location: s3_utility.cpp:539\ process: padbmaster [pid=30489]\ -----------------------------------------------\ \ org.postgresql.core.v3.QueryExecutorImpl.receiveErrorResponse(QueryExecutorImpl.java:2102)\ org.postgresql.core.v3.QueryExecutorImpl.processResults(QueryExecutorImpl.java:1835)\ org.postgresql.core.v3.QueryExecutorImpl.execute(QueryExecutorImpl.java:257)\ org.postgresql.jdbc2.AbstractJdbc2Statement.execute(AbstractJdbc2Statement.java:500)\ org.postgresql.jdbc2.AbstractJdbc2Statement.executeWithFlags(AbstractJdbc2Statement.java:388)\ org.postgresql.jdbc2.AbstractJdbc2Statement.execute(AbstractJdbc2Statement.java:381)\ scala.slick.jdbc.StatementInvoker.results(StatementInvoker.scala:38)\ scala.slick.jdbc.StatementInvoker.iteratorTo(StatementInvoker.scala:22)\ scala.slick.jdbc.Invoker$class.iterator(Invoker.scala:15)\ scala.slick.jdbc.StatementInvoker.iterator(StatementInvoker.scala:16)\ scala.slick.jdbc.Invoker$class.execute(Invoker.scala:23)\ scala.slick.jdbc.StatementInvoker.execute(StatementInvoker.scala:16)\ com.mindcandy.waterfall.io.RedshiftIOSink$$anonfun$storeFrom$1$$anonfun$apply$mcV$sp$2.apply$mcV$sp(RedshiftIO.scala:64)\ com.mindcandy.waterfall.io.RedshiftIOSink$$anonfun$storeFrom$1$$anonfun$apply$mcV$sp$2.apply(RedshiftIO.scala:56)\ com.mindcandy.waterfall.io.RedshiftIOSink$$anonfun$storeFrom$1$$anonfun$apply$mcV$sp$2.apply(RedshiftIO.scala:56)\ scala.util.DynamicVariable.withValue(DynamicVariable.scala:57)\ scala.slick.backend.DatabaseComponent$class.withDynamicSession(DatabaseComponent.scala:67)\ scala.slick.jdbc.JdbcBackend$.withDynamicSession(JdbcBackend.scala:448)\ scala.slick.backend.DatabaseComponent$DatabaseDef$$anonfun$withDynSession$1.apply(DatabaseComponent.scala:51)\ scala.slick.backend.DatabaseComponent$DatabaseDef$$anonfun$withDynSession$1.apply(DatabaseComponent.scala:51)\ scala.slick.backend.DatabaseComponent$DatabaseDef$class.withSession(DatabaseComponent.scala:34)\ scala.slick.jdbc.JdbcBackend$DatabaseFactoryDef$$anon$4.withSession(JdbcBackend.scala:61)\ scala.slick.backend.DatabaseComponent$DatabaseDef$class.withDynSession(DatabaseComponent.scala:51)\ scala.slick.jdbc.JdbcBackend$DatabaseFactoryDef$$anon$4.withDynSession(JdbcBackend.scala:61)\ com.mindcandy.waterfall.io.RedshiftIOSink$$anonfun$storeFrom$1.apply$mcV$sp(RedshiftIO.scala:56)\ com.mindcandy.waterfall.io.RedshiftIOSink$$anonfun$storeFrom$1.apply(RedshiftIO.scala:56)\ com.mindcandy.waterfall.io.RedshiftIOSink$$anonfun$storeFrom$1.apply(RedshiftIO.scala:56)\ scala.util.Try$.apply(Try.scala:161)\ com.mindcandy.waterfall.io.RedshiftIOSink.storeFrom(RedshiftIO.scala:56)\ com.mindcandy.waterfall.WaterfallDrop$$anonfun$run$1$$anonfun$apply$1.apply(WaterfallDrop.scala:23)\ com.mindcandy.waterfall.WaterfallDrop$$anonfun$run$1$$anonfun$apply$1.apply(WaterfallDrop.scala:22)\ scala.util.Success.flatMap(Try.scala:200)\ com.mindcandy.waterfall.WaterfallDrop$$anonfun$run$1.apply(WaterfallDrop.scala:22)\ com.mindcandy.waterfall.WaterfallDrop$$anonfun$run$1.apply(WaterfallDrop.scala:21)\ scala.util.Success.flatMap(Try.scala:200)\ com.mindcandy.waterfall.WaterfallDrop$class.run(WaterfallDrop.scala:21)\ com.mindcandy.waterfall.drop.FacebookInsightsToRedshiftDrop.run(FacebookInsightsToRedshiftDrop.scala:46)\ com.mindcandy.waterfall.actor.DropWorker$$anonfun$receive$1.applyOrElse(DropWorker.scala:19)\ akka.actor.Actor$class.aroundReceive(Actor.scala:465)\ com.mindcandy.waterfall.actor.DropWorker.aroundReceive(DropWorker.scala:14)\ akka.actor.ActorCell.receiveMessage(ActorCell.scala:516)\ akka.actor.ActorCell.invoke(ActorCell.scala:487)\ akka.dispatch.Mailbox.processMailbox(Mailbox.scala:238)\ akka.dispatch.Mailbox.run(Mailbox.scala:220)\ akka.dispatch.ForkJoinExecutorConfigurator$AkkaForkJoinTask.exec(AbstractDispatcher.scala:393)\ scala.concurrent.forkjoin.ForkJoinTask.doExec(ForkJoinTask.java:260)\ scala.concurrent.forkjoin.ForkJoinPool$WorkQueue.runTask(ForkJoinPool.java:1339)\ scala.concurrent.forkjoin.ForkJoinPool.runWorker(ForkJoinPool.java:1979)\ scala.concurrent.forkjoin.ForkJoinWorkerThread.run(ForkJoinWorkerThread.java:107)\ ",
"logOutput": null,
"endTime": "2014-11-18T01:13:00.139Z",
"runID": "3ca1922b-2db9-4315-b8aa-fb8b6d452b43",
"jobID": 4
},
{
"startTime": "2014-11-17T01:10:00.016Z",
"exception": null,
"logOutput": null,
"endTime": "2014-11-17T01:15:52.013Z",
"runID": "22c739a9-1d65-4978-b037-2066eb814ac7",
"jobID": 4
},
{
"startTime": "2014-11-16T01:10:00.026Z",
"exception": null,
"logOutput": null,
"endTime": "2014-11-16T01:13:45.552Z",
"runID": "58a3eb19-d622-4ddf-99a7-6b219f30ad8b",
"jobID": 4
},
{
"startTime": "2014-11-21T01:15:00.016Z",
"exception": "org.postgresql.util.PSQLException: ERROR: The specified S3 prefix 'warriors-20141119' does not exist\ Detail: \ -----------------------------------------------\ error: The specified S3 prefix 'warriors-20141119' does not exist\ code: 8001\ context: \ query: 175937\ location: s3_utility.cpp:539\ process: padbmaster [pid=23094]\ -----------------------------------------------\ \ org.postgresql.core.v3.QueryExecutorImpl.receiveErrorResponse(QueryExecutorImpl.java:2102)\ org.postgresql.core.v3.QueryExecutorImpl.processResults(QueryExecutorImpl.java:1835)\ org.postgresql.core.v3.QueryExecutorImpl.execute(QueryExecutorImpl.java:257)\ org.postgresql.jdbc2.AbstractJdbc2Statement.execute(AbstractJdbc2Statement.java:500)\ org.postgresql.jdbc2.AbstractJdbc2Statement.executeWithFlags(AbstractJdbc2Statement.java:388)\ org.postgresql.jdbc2.AbstractJdbc2Statement.execute(AbstractJdbc2Statement.java:381)\ scala.slick.jdbc.StatementInvoker.results(StatementInvoker.scala:38)\ scala.slick.jdbc.StatementInvoker.iteratorTo(StatementInvoker.scala:22)\ scala.slick.jdbc.Invoker$class.iterator(Invoker.scala:15)\ scala.slick.jdbc.StatementInvoker.iterator(StatementInvoker.scala:16)\ scala.slick.jdbc.Invoker$class.execute(Invoker.scala:23)\ scala.slick.jdbc.StatementInvoker.execute(StatementInvoker.scala:16)\ com.mindcandy.waterfall.io.RedshiftIOSink$$anonfun$storeFrom$1$$anonfun$apply$mcV$sp$2.apply$mcV$sp(RedshiftIO.scala:64)\ com.mindcandy.waterfall.io.RedshiftIOSink$$anonfun$storeFrom$1$$anonfun$apply$mcV$sp$2.apply(RedshiftIO.scala:56)\ com.mindcandy.waterfall.io.RedshiftIOSink$$anonfun$storeFrom$1$$anonfun$apply$mcV$sp$2.apply(RedshiftIO.scala:56)\ scala.util.DynamicVariable.withValue(DynamicVariable.scala:57)\ scala.slick.backend.DatabaseComponent$class.withDynamicSession(DatabaseComponent.scala:67)\ scala.slick.jdbc.JdbcBackend$.withDynamicSession(JdbcBackend.scala:448)\ scala.slick.backend.DatabaseComponent$DatabaseDef$$anonfun$withDynSession$1.apply(DatabaseComponent.scala:51)\ scala.slick.backend.DatabaseComponent$DatabaseDef$$anonfun$withDynSession$1.apply(DatabaseComponent.scala:51)\ scala.slick.backend.DatabaseComponent$DatabaseDef$class.withSession(DatabaseComponent.scala:34)\ scala.slick.jdbc.JdbcBackend$DatabaseFactoryDef$$anon$4.withSession(JdbcBackend.scala:61)\ scala.slick.backend.DatabaseComponent$DatabaseDef$class.withDynSession(DatabaseComponent.scala:51)\ scala.slick.jdbc.JdbcBackend$DatabaseFactoryDef$$anon$4.withDynSession(JdbcBackend.scala:61)\ com.mindcandy.waterfall.io.RedshiftIOSink$$anonfun$storeFrom$1.apply$mcV$sp(RedshiftIO.scala:56)\ com.mindcandy.waterfall.io.RedshiftIOSink$$anonfun$storeFrom$1.apply(RedshiftIO.scala:56)\ com.mindcandy.waterfall.io.RedshiftIOSink$$anonfun$storeFrom$1.apply(RedshiftIO.scala:56)\ scala.util.Try$.apply(Try.scala:161)\ com.mindcandy.waterfall.io.RedshiftIOSink.storeFrom(RedshiftIO.scala:56)\ com.mindcandy.waterfall.WaterfallDrop$$anonfun$run$1$$anonfun$apply$1.apply(WaterfallDrop.scala:23)\ com.mindcandy.waterfall.WaterfallDrop$$anonfun$run$1$$anonfun$apply$1.apply(WaterfallDrop.scala:22)\ scala.util.Success.flatMap(Try.scala:200)\ com.mindcandy.waterfall.WaterfallDrop$$anonfun$run$1.apply(WaterfallDrop.scala:22)\ com.mindcandy.waterfall.WaterfallDrop$$anonfun$run$1.apply(WaterfallDrop.scala:21)\ scala.util.Success.flatMap(Try.scala:200)\ com.mindcandy.waterfall.WaterfallDrop$class.run(WaterfallDrop.scala:21)\ com.mindcandy.waterfall.drop.FacebookInsightsToRedshiftDrop.run(FacebookInsightsToRedshiftDrop.scala:46)\ com.mindcandy.waterfall.actor.DropWorker$$anonfun$receive$1.applyOrElse(DropWorker.scala:19)\ akka.actor.Actor$class.aroundReceive(Actor.scala:465)\ com.mindcandy.waterfall.actor.DropWorker.aroundReceive(DropWorker.scala:14)\ akka.actor.ActorCell.receiveMessage(ActorCell.scala:516)\ akka.actor.ActorCell.invoke(ActorCell.scala:487)\ akka.dispatch.Mailbox.processMailbox(Mailbox.scala:238)\ akka.dispatch.Mailbox.run(Mailbox.scala:220)\ akka.dispatch.ForkJoinExecutorConfigurator$AkkaForkJoinTask.exec(AbstractDispatcher.scala:393)\ scala.concurrent.forkjoin.ForkJoinTask.doExec(ForkJoinTask.java:260)\ scala.concurrent.forkjoin.ForkJoinPool$WorkQueue.runTask(ForkJoinPool.java:1339)\ scala.concurrent.forkjoin.ForkJoinPool.runWorker(ForkJoinPool.java:1979)\ scala.concurrent.forkjoin.ForkJoinWorkerThread.run(ForkJoinWorkerThread.java:107)\ ",
"logOutput": null,
"endTime": "2014-11-21T01:15:56.786Z",
"runID": "bcf3e31e-abf4-4c38-9de9-51857219c1e7",
"jobID": 24
},
{
"startTime": "2014-11-20T01:15:00.016Z",
"exception": "org.postgresql.util.PSQLException: ERROR: The specified S3 prefix 'warriors-20141118' does not exist\ Detail: \ -----------------------------------------------\ error: The specified S3 prefix 'warriors-20141118' does not exist\ code: 8001\ context: \ query: 155515\ location: s3_utility.cpp:539\ process: padbmaster [pid=32671]\ -----------------------------------------------\ \ org.postgresql.core.v3.QueryExecutorImpl.receiveErrorResponse(QueryExecutorImpl.java:2102)\ org.postgresql.core.v3.QueryExecutorImpl.processResults(QueryExecutorImpl.java:1835)\ org.postgresql.core.v3.QueryExecutorImpl.execute(QueryExecutorImpl.java:257)\ org.postgresql.jdbc2.AbstractJdbc2Statement.execute(AbstractJdbc2Statement.java:500)\ org.postgresql.jdbc2.AbstractJdbc2Statement.executeWithFlags(AbstractJdbc2Statement.java:388)\ org.postgresql.jdbc2.AbstractJdbc2Statement.execute(AbstractJdbc2Statement.java:381)\ scala.slick.jdbc.StatementInvoker.results(StatementInvoker.scala:38)\ scala.slick.jdbc.StatementInvoker.iteratorTo(StatementInvoker.scala:22)\ scala.slick.jdbc.Invoker$class.iterator(Invoker.scala:15)\ scala.slick.jdbc.StatementInvoker.iterator(StatementInvoker.scala:16)\ scala.slick.jdbc.Invoker$class.execute(Invoker.scala:23)\ scala.slick.jdbc.StatementInvoker.execute(StatementInvoker.scala:16)\ com.mindcandy.waterfall.io.RedshiftIOSink$$anonfun$storeFrom$1$$anonfun$apply$mcV$sp$2.apply$mcV$sp(RedshiftIO.scala:64)\ com.mindcandy.waterfall.io.RedshiftIOSink$$anonfun$storeFrom$1$$anonfun$apply$mcV$sp$2.apply(RedshiftIO.scala:56)\ com.mindcandy.waterfall.io.RedshiftIOSink$$anonfun$storeFrom$1$$anonfun$apply$mcV$sp$2.apply(RedshiftIO.scala:56)\ scala.util.DynamicVariable.withValue(DynamicVariable.scala:57)\ scala.slick.backend.DatabaseComponent$class.withDynamicSession(DatabaseComponent.scala:67)\ scala.slick.jdbc.JdbcBackend$.withDynamicSession(JdbcBackend.scala:448)\ scala.slick.backend.DatabaseComponent$DatabaseDef$$anonfun$withDynSession$1.apply(DatabaseComponent.scala:51)\ scala.slick.backend.DatabaseComponent$DatabaseDef$$anonfun$withDynSession$1.apply(DatabaseComponent.scala:51)\ scala.slick.backend.DatabaseComponent$DatabaseDef$class.withSession(DatabaseComponent.scala:34)\ scala.slick.jdbc.JdbcBackend$DatabaseFactoryDef$$anon$4.withSession(JdbcBackend.scala:61)\ scala.slick.backend.DatabaseComponent$DatabaseDef$class.withDynSession(DatabaseComponent.scala:51)\ scala.slick.jdbc.JdbcBackend$DatabaseFactoryDef$$anon$4.withDynSession(JdbcBackend.scala:61)\ com.mindcandy.waterfall.io.RedshiftIOSink$$anonfun$storeFrom$1.apply$mcV$sp(RedshiftIO.scala:56)\ com.mindcandy.waterfall.io.RedshiftIOSink$$anonfun$storeFrom$1.apply(RedshiftIO.scala:56)\ com.mindcandy.waterfall.io.RedshiftIOSink$$anonfun$storeFrom$1.apply(RedshiftIO.scala:56)\ scala.util.Try$.apply(Try.scala:161)\ com.mindcandy.waterfall.io.RedshiftIOSink.storeFrom(RedshiftIO.scala:56)\ com.mindcandy.waterfall.WaterfallDrop$$anonfun$run$1$$anonfun$apply$1.apply(WaterfallDrop.scala:23)\ com.mindcandy.waterfall.WaterfallDrop$$anonfun$run$1$$anonfun$apply$1.apply(WaterfallDrop.scala:22)\ scala.util.Success.flatMap(Try.scala:200)\ com.mindcandy.waterfall.WaterfallDrop$$anonfun$run$1.apply(WaterfallDrop.scala:22)\ com.mindcandy.waterfall.WaterfallDrop$$anonfun$run$1.apply(WaterfallDrop.scala:21)\ scala.util.Success.flatMap(Try.scala:200)\ com.mindcandy.waterfall.WaterfallDrop$class.run(WaterfallDrop.scala:21)\ com.mindcandy.waterfall.drop.FacebookInsightsToRedshiftDrop.run(FacebookInsightsToRedshiftDrop.scala:46)\ com.mindcandy.waterfall.actor.DropWorker$$anonfun$receive$1.applyOrElse(DropWorker.scala:19)\ akka.actor.Actor$class.aroundReceive(Actor.scala:465)\ com.mindcandy.waterfall.actor.DropWorker.aroundReceive(DropWorker.scala:14)\ akka.actor.ActorCell.receiveMessage(ActorCell.scala:516)\ akka.actor.ActorCell.invoke(ActorCell.scala:487)\ akka.dispatch.Mailbox.processMailbox(Mailbox.scala:238)\ akka.dispatch.Mailbox.run(Mailbox.scala:220)\ akka.dispatch.ForkJoinExecutorConfigurator$AkkaForkJoinTask.exec(AbstractDispatcher.scala:393)\ scala.concurrent.forkjoin.ForkJoinTask.doExec(ForkJoinTask.java:260)\ scala.concurrent.forkjoin.ForkJoinPool$WorkQueue.runTask(ForkJoinPool.java:1339)\ scala.concurrent.forkjoin.ForkJoinPool.runWorker(ForkJoinPool.java:1979)\ scala.concurrent.forkjoin.ForkJoinWorkerThread.run(ForkJoinWorkerThread.java:107)\ ",
"logOutput": null,
"endTime": "2014-11-20T01:15:16.414Z",
"runID": "4f0a20f0-6fcf-4ef1-8bdf-6bf951836cbd",
"jobID": 24
},
{
"startTime": "2014-11-19T01:15:00.016Z",
"exception": "org.postgresql.util.PSQLException: ERROR: The specified S3 prefix 'warriors-20141117' does not exist\ Detail: \ -----------------------------------------------\ error: The specified S3 prefix 'warriors-20141117' does not exist\ code: 8001\ context: \ query: 136390\ location: s3_utility.cpp:539\ process: padbmaster [pid=2725]\ -----------------------------------------------\ \ org.postgresql.core.v3.QueryExecutorImpl.receiveErrorResponse(QueryExecutorImpl.java:2102)\ org.postgresql.core.v3.QueryExecutorImpl.processResults(QueryExecutorImpl.java:1835)\ org.postgresql.core.v3.QueryExecutorImpl.execute(QueryExecutorImpl.java:257)\ org.postgresql.jdbc2.AbstractJdbc2Statement.execute(AbstractJdbc2Statement.java:500)\ org.postgresql.jdbc2.AbstractJdbc2Statement.executeWithFlags(AbstractJdbc2Statement.java:388)\ org.postgresql.jdbc2.AbstractJdbc2Statement.execute(AbstractJdbc2Statement.java:381)\ scala.slick.jdbc.StatementInvoker.results(StatementInvoker.scala:38)\ scala.slick.jdbc.StatementInvoker.iteratorTo(StatementInvoker.scala:22)\ scala.slick.jdbc.Invoker$class.iterator(Invoker.scala:15)\ scala.slick.jdbc.StatementInvoker.iterator(StatementInvoker.scala:16)\ scala.slick.jdbc.Invoker$class.execute(Invoker.scala:23)\ scala.slick.jdbc.StatementInvoker.execute(StatementInvoker.scala:16)\ com.mindcandy.waterfall.io.RedshiftIOSink$$anonfun$storeFrom$1$$anonfun$apply$mcV$sp$2.apply$mcV$sp(RedshiftIO.scala:64)\ com.mindcandy.waterfall.io.RedshiftIOSink$$anonfun$storeFrom$1$$anonfun$apply$mcV$sp$2.apply(RedshiftIO.scala:56)\ com.mindcandy.waterfall.io.RedshiftIOSink$$anonfun$storeFrom$1$$anonfun$apply$mcV$sp$2.apply(RedshiftIO.scala:56)\ scala.util.DynamicVariable.withValue(DynamicVariable.scala:57)\ scala.slick.backend.DatabaseComponent$class.withDynamicSession(DatabaseComponent.scala:67)\ scala.slick.jdbc.JdbcBackend$.withDynamicSession(JdbcBackend.scala:448)\ scala.slick.backend.DatabaseComponent$DatabaseDef$$anonfun$withDynSession$1.apply(DatabaseComponent.scala:51)\ scala.slick.backend.DatabaseComponent$DatabaseDef$$anonfun$withDynSession$1.apply(DatabaseComponent.scala:51)\ scala.slick.backend.DatabaseComponent$DatabaseDef$class.withSession(DatabaseComponent.scala:34)\ scala.slick.jdbc.JdbcBackend$DatabaseFactoryDef$$anon$4.withSession(JdbcBackend.scala:61)\ scala.slick.backend.DatabaseComponent$DatabaseDef$class.withDynSession(DatabaseComponent.scala:51)\ scala.slick.jdbc.JdbcBackend$DatabaseFactoryDef$$anon$4.withDynSession(JdbcBackend.scala:61)\ com.mindcandy.waterfall.io.RedshiftIOSink$$anonfun$storeFrom$1.apply$mcV$sp(RedshiftIO.scala:56)\ com.mindcandy.waterfall.io.RedshiftIOSink$$anonfun$storeFrom$1.apply(RedshiftIO.scala:56)\ com.mindcandy.waterfall.io.RedshiftIOSink$$anonfun$storeFrom$1.apply(RedshiftIO.scala:56)\ scala.util.Try$.apply(Try.scala:161)\ com.mindcandy.waterfall.io.RedshiftIOSink.storeFrom(RedshiftIO.scala:56)\ com.mindcandy.waterfall.WaterfallDrop$$anonfun$run$1$$anonfun$apply$1.apply(WaterfallDrop.scala:23)\ com.mindcandy.waterfall.WaterfallDrop$$anonfun$run$1$$anonfun$apply$1.apply(WaterfallDrop.scala:22)\ scala.util.Success.flatMap(Try.scala:200)\ com.mindcandy.waterfall.WaterfallDrop$$anonfun$run$1.apply(WaterfallDrop.scala:22)\ com.mindcandy.waterfall.WaterfallDrop$$anonfun$run$1.apply(WaterfallDrop.scala:21)\ scala.util.Success.flatMap(Try.scala:200)\ com.mindcandy.waterfall.WaterfallDrop$class.run(WaterfallDrop.scala:21)\ com.mindcandy.waterfall.drop.FacebookInsightsToRedshiftDrop.run(FacebookInsightsToRedshiftDrop.scala:46)\ com.mindcandy.waterfall.actor.DropWorker$$anonfun$receive$1.applyOrElse(DropWorker.scala:19)\ akka.actor.Actor$class.aroundReceive(Actor.scala:465)\ com.mindcandy.waterfall.actor.DropWorker.aroundReceive(DropWorker.scala:14)\ akka.actor.ActorCell.receiveMessage(ActorCell.scala:516)\ akka.actor.ActorCell.invoke(ActorCell.scala:487)\ akka.dispatch.Mailbox.processMailbox(Mailbox.scala:238)\ akka.dispatch.Mailbox.run(Mailbox.scala:220)\ akka.dispatch.ForkJoinExecutorConfigurator$AkkaForkJoinTask.exec(AbstractDispatcher.scala:393)\ scala.concurrent.forkjoin.ForkJoinTask.doExec(ForkJoinTask.java:260)\ scala.concurrent.forkjoin.ForkJoinPool$WorkQueue.runTask(ForkJoinPool.java:1339)\ scala.concurrent.forkjoin.ForkJoinPool.runWorker(ForkJoinPool.java:1979)\ scala.concurrent.forkjoin.ForkJoinWorkerThread.run(ForkJoinWorkerThread.java:107)\ ",
"logOutput": null,
"endTime": "2014-11-19T01:15:15.829Z",
"runID": "7cb620c1-a9e1-41d3-8f46-7597106a4e34",
"jobID": 24
},
{
"startTime": "2014-11-18T01:15:00.016Z",
"exception": "org.postgresql.util.PSQLException: ERROR: The specified S3 prefix 'warriors-20141116' does not exist\ Detail: \ -----------------------------------------------\ error: The specified S3 prefix 'warriors-20141116' does not exist\ code: 8001\ context: \ query: 117584\ location: s3_utility.cpp:539\ process: padbmaster [pid=30793]\ -----------------------------------------------\ \ org.postgresql.core.v3.QueryExecutorImpl.receiveErrorResponse(QueryExecutorImpl.java:2102)\ org.postgresql.core.v3.QueryExecutorImpl.processResults(QueryExecutorImpl.java:1835)\ org.postgresql.core.v3.QueryExecutorImpl.execute(QueryExecutorImpl.java:257)\ org.postgresql.jdbc2.AbstractJdbc2Statement.execute(AbstractJdbc2Statement.java:500)\ org.postgresql.jdbc2.AbstractJdbc2Statement.executeWithFlags(AbstractJdbc2Statement.java:388)\ org.postgresql.jdbc2.AbstractJdbc2Statement.execute(AbstractJdbc2Statement.java:381)\ scala.slick.jdbc.StatementInvoker.results(StatementInvoker.scala:38)\ scala.slick.jdbc.StatementInvoker.iteratorTo(StatementInvoker.scala:22)\ scala.slick.jdbc.Invoker$class.iterator(Invoker.scala:15)\ scala.slick.jdbc.StatementInvoker.iterator(StatementInvoker.scala:16)\ scala.slick.jdbc.Invoker$class.execute(Invoker.scala:23)\ scala.slick.jdbc.StatementInvoker.execute(StatementInvoker.scala:16)\ com.mindcandy.waterfall.io.RedshiftIOSink$$anonfun$storeFrom$1$$anonfun$apply$mcV$sp$2.apply$mcV$sp(RedshiftIO.scala:64)\ com.mindcandy.waterfall.io.RedshiftIOSink$$anonfun$storeFrom$1$$anonfun$apply$mcV$sp$2.apply(RedshiftIO.scala:56)\ com.mindcandy.waterfall.io.RedshiftIOSink$$anonfun$storeFrom$1$$anonfun$apply$mcV$sp$2.apply(RedshiftIO.scala:56)\ scala.util.DynamicVariable.withValue(DynamicVariable.scala:57)\ scala.slick.backend.DatabaseComponent$class.withDynamicSession(DatabaseComponent.scala:67)\ scala.slick.jdbc.JdbcBackend$.withDynamicSession(JdbcBackend.scala:448)\ scala.slick.backend.DatabaseComponent$DatabaseDef$$anonfun$withDynSession$1.apply(DatabaseComponent.scala:51)\ scala.slick.backend.DatabaseComponent$DatabaseDef$$anonfun$withDynSession$1.apply(DatabaseComponent.scala:51)\ scala.slick.backend.DatabaseComponent$DatabaseDef$class.withSession(DatabaseComponent.scala:34)\ scala.slick.jdbc.JdbcBackend$DatabaseFactoryDef$$anon$4.withSession(JdbcBackend.scala:61)\ scala.slick.backend.DatabaseComponent$DatabaseDef$class.withDynSession(DatabaseComponent.scala:51)\ scala.slick.jdbc.JdbcBackend$DatabaseFactoryDef$$anon$4.withDynSession(JdbcBackend.scala:61)\ com.mindcandy.waterfall.io.RedshiftIOSink$$anonfun$storeFrom$1.apply$mcV$sp(RedshiftIO.scala:56)\ com.mindcandy.waterfall.io.RedshiftIOSink$$anonfun$storeFrom$1.apply(RedshiftIO.scala:56)\ com.mindcandy.waterfall.io.RedshiftIOSink$$anonfun$storeFrom$1.apply(RedshiftIO.scala:56)\ scala.util.Try$.apply(Try.scala:161)\ com.mindcandy.waterfall.io.RedshiftIOSink.storeFrom(RedshiftIO.scala:56)\ com.mindcandy.waterfall.WaterfallDrop$$anonfun$run$1$$anonfun$apply$1.apply(WaterfallDrop.scala:23)\ com.mindcandy.waterfall.WaterfallDrop$$anonfun$run$1$$anonfun$apply$1.apply(WaterfallDrop.scala:22)\ scala.util.Success.flatMap(Try.scala:200)\ com.mindcandy.waterfall.WaterfallDrop$$anonfun$run$1.apply(WaterfallDrop.scala:22)\ com.mindcandy.waterfall.WaterfallDrop$$anonfun$run$1.apply(WaterfallDrop.scala:21)\ scala.util.Success.flatMap(Try.scala:200)\ com.mindcandy.waterfall.WaterfallDrop$class.run(WaterfallDrop.scala:21)\ com.mindcandy.waterfall.drop.FacebookInsightsToRedshiftDrop.run(FacebookInsightsToRedshiftDrop.scala:46)\ com.mindcandy.waterfall.actor.DropWorker$$anonfun$receive$1.applyOrElse(DropWorker.scala:19)\ akka.actor.Actor$class.aroundReceive(Actor.scala:465)\ com.mindcandy.waterfall.actor.DropWorker.aroundReceive(DropWorker.scala:14)\ akka.actor.ActorCell.receiveMessage(ActorCell.scala:516)\ akka.actor.ActorCell.invoke(ActorCell.scala:487)\ akka.dispatch.Mailbox.processMailbox(Mailbox.scala:238)\ akka.dispatch.Mailbox.run(Mailbox.scala:220)\ akka.dispatch.ForkJoinExecutorConfigurator$AkkaForkJoinTask.exec(AbstractDispatcher.scala:393)\ scala.concurrent.forkjoin.ForkJoinTask.doExec(ForkJoinTask.java:260)\ scala.concurrent.forkjoin.ForkJoinPool$WorkQueue.runTask(ForkJoinPool.java:1339)\ scala.concurrent.forkjoin.ForkJoinPool.runWorker(ForkJoinPool.java:1979)\ scala.concurrent.forkjoin.ForkJoinWorkerThread.run(ForkJoinWorkerThread.java:107)\ ",
"logOutput": null,
"endTime": "2014-11-18T01:27:35.103Z",
"runID": "d756e176-14f3-4e08-9267-c7e7e46df073",
"jobID": 24
},
{
"startTime": "2014-11-17T01:15:00.016Z",
"exception": null,
"logOutput": null,
"endTime": "2014-11-17T01:19:12.675Z",
"runID": "bb7979cb-7945-4560-8106-52f1d88674b8",
"jobID": 24
},
{
"startTime": "2014-11-16T01:15:00.017Z",
"exception": null,
"logOutput": null,
"endTime": "2014-11-16T01:18:07.957Z",
"runID": "07955edf-4432-4a3c-85dc-0047f367db8c",
"jobID": 24
},
{
"startTime": "2014-11-15T01:15:00.017Z",
"exception": null,
"logOutput": null,
"endTime": "2014-11-15T01:19:06.627Z",
"runID": "fce8f7da-1daf-4256-abf4-6b1135945258",
"jobID": 24
},
{
"startTime": "2014-11-21T05:55:00.042Z",
"exception": "org.postgresql.util.PSQLException: ERROR: The specified S3 prefix 'google-play-reports-20141120' does not exist\ Detail: \ -----------------------------------------------\ error: The specified S3 prefix 'google-play-reports-20141120' does not exist\ code: 8001\ context: \ query: 180203\ location: s3_utility.cpp:539\ process: padbmaster [pid=7568]\ -----------------------------------------------\ \ org.postgresql.core.v3.QueryExecutorImpl.receiveErrorResponse(QueryExecutorImpl.java:2102)\ org.postgresql.core.v3.QueryExecutorImpl.processResults(QueryExecutorImpl.java:1835)\ org.postgresql.core.v3.QueryExecutorImpl.execute(QueryExecutorImpl.java:257)\ org.postgresql.jdbc2.AbstractJdbc2Statement.execute(AbstractJdbc2Statement.java:500)\ org.postgresql.jdbc2.AbstractJdbc2Statement.executeWithFlags(AbstractJdbc2Statement.java:388)\ org.postgresql.jdbc2.AbstractJdbc2Statement.execute(AbstractJdbc2Statement.java:381)\ scala.slick.jdbc.StatementInvoker.results(StatementInvoker.scala:38)\ scala.slick.jdbc.StatementInvoker.iteratorTo(StatementInvoker.scala:22)\ scala.slick.jdbc.Invoker$class.iterator(Invoker.scala:15)\ scala.slick.jdbc.StatementInvoker.iterator(StatementInvoker.scala:16)\ scala.slick.jdbc.Invoker$class.execute(Invoker.scala:23)\ scala.slick.jdbc.StatementInvoker.execute(StatementInvoker.scala:16)\ com.mindcandy.waterfall.io.RedshiftIOSink$$anonfun$storeFrom$1$$anonfun$apply$mcV$sp$2.apply$mcV$sp(RedshiftIO.scala:64)\ com.mindcandy.waterfall.io.RedshiftIOSink$$anonfun$storeFrom$1$$anonfun$apply$mcV$sp$2.apply(RedshiftIO.scala:56)\ com.mindcandy.waterfall.io.RedshiftIOSink$$anonfun$storeFrom$1$$anonfun$apply$mcV$sp$2.apply(RedshiftIO.scala:56)\ scala.util.DynamicVariable.withValue(DynamicVariable.scala:57)\ scala.slick.backend.DatabaseComponent$class.withDynamicSession(DatabaseComponent.scala:67)\ scala.slick.jdbc.JdbcBackend$.withDynamicSession(JdbcBackend.scala:448)\ scala.slick.backend.DatabaseComponent$DatabaseDef$$anonfun$withDynSession$1.apply(DatabaseComponent.scala:51)\ scala.slick.backend.DatabaseComponent$DatabaseDef$$anonfun$withDynSession$1.apply(DatabaseComponent.scala:51)\ scala.slick.backend.DatabaseComponent$DatabaseDef$class.withSession(DatabaseComponent.scala:34)\ scala.slick.jdbc.JdbcBackend$DatabaseFactoryDef$$anon$4.withSession(JdbcBackend.scala:61)\ scala.slick.backend.DatabaseComponent$DatabaseDef$class.withDynSession(DatabaseComponent.scala:51)\ scala.slick.jdbc.JdbcBackend$DatabaseFactoryDef$$anon$4.withDynSession(JdbcBackend.scala:61)\ com.mindcandy.waterfall.io.RedshiftIOSink$$anonfun$storeFrom$1.apply$mcV$sp(RedshiftIO.scala:56)\ com.mindcandy.waterfall.io.RedshiftIOSink$$anonfun$storeFrom$1.apply(RedshiftIO.scala:56)\ com.mindcandy.waterfall.io.RedshiftIOSink$$anonfun$storeFrom$1.apply(RedshiftIO.scala:56)\ scala.util.Try$.apply(Try.scala:161)\ com.mindcandy.waterfall.io.RedshiftIOSink.storeFrom(RedshiftIO.scala:56)\ com.mindcandy.waterfall.WaterfallDrop$$anonfun$run$1$$anonfun$apply$1.apply(WaterfallDrop.scala:23)\ com.mindcandy.waterfall.WaterfallDrop$$anonfun$run$1$$anonfun$apply$1.apply(WaterfallDrop.scala:22)\ scala.util.Success.flatMap(Try.scala:200)\ com.mindcandy.waterfall.WaterfallDrop$$anonfun$run$1.apply(WaterfallDrop.scala:22)\ com.mindcandy.waterfall.WaterfallDrop$$anonfun$run$1.apply(WaterfallDrop.scala:21)\ scala.util.Success.flatMap(Try.scala:200)\ com.mindcandy.waterfall.WaterfallDrop$class.run(WaterfallDrop.scala:21)\ com.mindcandy.waterfall.drop.GooglePlayToRedshiftDrop.run(GooglePlayToRedshiftDrop.scala:180)\ com.mindcandy.waterfall.actor.DropWorker$$anonfun$receive$1.applyOrElse(DropWorker.scala:19)\ akka.actor.Actor$class.aroundReceive(Actor.scala:465)\ com.mindcandy.waterfall.actor.DropWorker.aroundReceive(DropWorker.scala:14)\ akka.actor.ActorCell.receiveMessage(ActorCell.scala:516)\ akka.actor.ActorCell.invoke(ActorCell.scala:487)\ akka.dispatch.Mailbox.processMailbox(Mailbox.scala:238)\ akka.dispatch.Mailbox.run(Mailbox.scala:220)\ akka.dispatch.ForkJoinExecutorConfigurator$AkkaForkJoinTask.exec(AbstractDispatcher.scala:393)\ scala.concurrent.forkjoin.ForkJoinTask.doExec(ForkJoinTask.java:260)\ scala.concurrent.forkjoin.ForkJoinPool$WorkQueue.runTask(ForkJoinPool.java:1339)\ scala.concurrent.forkjoin.ForkJoinPool.runWorker(ForkJoinPool.java:1979)\ scala.concurrent.forkjoin.ForkJoinWorkerThread.run(ForkJoinWorkerThread.java:107)\ ",
"logOutput": null,
"endTime": "2014-11-21T05:55:32.495Z",
"runID": "027773a6-9aa6-4ba8-8e92-b5761e03bd0e",
"jobID": 23
},
{
"startTime": "2014-11-20T11:40:24.516Z",
"exception": "org.postgresql.util.PSQLException: ERROR: The specified S3 prefix 'google-play-reports-20141119' does not exist\ Detail: \ -----------------------------------------------\ error: The specified S3 prefix 'google-play-reports-20141119' does not exist\ code: 8001\ context: \ query: 164659\ location: s3_utility.cpp:539\ process: padbmaster [pid=5585]\ -----------------------------------------------\ \ org.postgresql.core.v3.QueryExecutorImpl.receiveErrorResponse(QueryExecutorImpl.java:2102)\ org.postgresql.core.v3.QueryExecutorImpl.processResults(QueryExecutorImpl.java:1835)\ org.postgresql.core.v3.QueryExecutorImpl.execute(QueryExecutorImpl.java:257)\ org.postgresql.jdbc2.AbstractJdbc2Statement.execute(AbstractJdbc2Statement.java:500)\ org.postgresql.jdbc2.AbstractJdbc2Statement.executeWithFlags(AbstractJdbc2Statement.java:388)\ org.postgresql.jdbc2.AbstractJdbc2Statement.execute(AbstractJdbc2Statement.java:381)\ scala.slick.jdbc.StatementInvoker.results(StatementInvoker.scala:38)\ scala.slick.jdbc.StatementInvoker.iteratorTo(StatementInvoker.scala:22)\ scala.slick.jdbc.Invoker$class.iterator(Invoker.scala:15)\ scala.slick.jdbc.StatementInvoker.iterator(StatementInvoker.scala:16)\ scala.slick.jdbc.Invoker$class.execute(Invoker.scala:23)\ scala.slick.jdbc.StatementInvoker.execute(StatementInvoker.scala:16)\ com.mindcandy.waterfall.io.RedshiftIOSink$$anonfun$storeFrom$1$$anonfun$apply$mcV$sp$2.apply$mcV$sp(RedshiftIO.scala:64)\ com.mindcandy.waterfall.io.RedshiftIOSink$$anonfun$storeFrom$1$$anonfun$apply$mcV$sp$2.apply(RedshiftIO.scala:56)\ com.mindcandy.waterfall.io.RedshiftIOSink$$anonfun$storeFrom$1$$anonfun$apply$mcV$sp$2.apply(RedshiftIO.scala:56)\ scala.util.DynamicVariable.withValue(DynamicVariable.scala:57)\ scala.slick.backend.DatabaseComponent$class.withDynamicSession(DatabaseComponent.scala:67)\ scala.slick.jdbc.JdbcBackend$.withDynamicSession(JdbcBackend.scala:448)\ scala.slick.backend.DatabaseComponent$DatabaseDef$$anonfun$withDynSession$1.apply(DatabaseComponent.scala:51)\ scala.slick.backend.DatabaseComponent$DatabaseDef$$anonfun$withDynSession$1.apply(DatabaseComponent.scala:51)\ scala.slick.backend.DatabaseComponent$DatabaseDef$class.withSession(DatabaseComponent.scala:34)\ scala.slick.jdbc.JdbcBackend$DatabaseFactoryDef$$anon$4.withSession(JdbcBackend.scala:61)\ scala.slick.backend.DatabaseComponent$DatabaseDef$class.withDynSession(DatabaseComponent.scala:51)\ scala.slick.jdbc.JdbcBackend$DatabaseFactoryDef$$anon$4.withDynSession(JdbcBackend.scala:61)\ com.mindcandy.waterfall.io.RedshiftIOSink$$anonfun$storeFrom$1.apply$mcV$sp(RedshiftIO.scala:56)\ com.mindcandy.waterfall.io.RedshiftIOSink$$anonfun$storeFrom$1.apply(RedshiftIO.scala:56)\ com.mindcandy.waterfall.io.RedshiftIOSink$$anonfun$storeFrom$1.apply(RedshiftIO.scala:56)\ scala.util.Try$.apply(Try.scala:161)\ com.mindcandy.waterfall.io.RedshiftIOSink.storeFrom(RedshiftIO.scala:56)\ com.mindcandy.waterfall.WaterfallDrop$$anonfun$run$1$$anonfun$apply$1.apply(WaterfallDrop.scala:23)\ com.mindcandy.waterfall.WaterfallDrop$$anonfun$run$1$$anonfun$apply$1.apply(WaterfallDrop.scala:22)\ scala.util.Success.flatMap(Try.scala:200)\ com.mindcandy.waterfall.WaterfallDrop$$anonfun$run$1.apply(WaterfallDrop.scala:22)\ com.mindcandy.waterfall.WaterfallDrop$$anonfun$run$1.apply(WaterfallDrop.scala:21)\ scala.util.Success.flatMap(Try.scala:200)\ com.mindcandy.waterfall.WaterfallDrop$class.run(WaterfallDrop.scala:21)\ com.mindcandy.waterfall.drop.GooglePlayToRedshiftDrop.run(GooglePlayToRedshiftDrop.scala:180)\ com.mindcandy.waterfall.actor.DropWorker$$anonfun$receive$1.applyOrElse(DropWorker.scala:19)\ akka.actor.Actor$class.aroundReceive(Actor.scala:465)\ com.mindcandy.waterfall.actor.DropWorker.aroundReceive(DropWorker.scala:14)\ akka.actor.ActorCell.receiveMessage(ActorCell.scala:516)\ akka.actor.ActorCell.invoke(ActorCell.scala:487)\ akka.dispatch.Mailbox.processMailbox(Mailbox.scala:238)\ akka.dispatch.Mailbox.run(Mailbox.scala:220)\ akka.dispatch.ForkJoinExecutorConfigurator$AkkaForkJoinTask.exec(AbstractDispatcher.scala:393)\ scala.concurrent.forkjoin.ForkJoinTask.doExec(ForkJoinTask.java:260)\ scala.concurrent.forkjoin.ForkJoinPool$WorkQueue.runTask(ForkJoinPool.java:1339)\ scala.concurrent.forkjoin.ForkJoinPool.runWorker(ForkJoinPool.java:1979)\ scala.concurrent.forkjoin.ForkJoinWorkerThread.run(ForkJoinWorkerThread.java:107)\ ",
"logOutput": null,
"endTime": "2014-11-20T11:40:39.704Z",
"runID": "f91ce191-1f2d-4f62-bc45-651b67784485",
"jobID": 23
},
{
"startTime": "2014-11-20T05:55:00.026Z",
"exception": "org.postgresql.util.PSQLException: ERROR: The specified S3 prefix 'google-play-reports-20141119' does not exist\ Detail: \ -----------------------------------------------\ error: The specified S3 prefix 'google-play-reports-20141119' does not exist\ code: 8001\ context: \ query: 159883\ location: s3_utility.cpp:539\ process: padbmaster [pid=17114]\ -----------------------------------------------\ \ org.postgresql.core.v3.QueryExecutorImpl.receiveErrorResponse(QueryExecutorImpl.java:2102)\ org.postgresql.core.v3.QueryExecutorImpl.processResults(QueryExecutorImpl.java:1835)\ org.postgresql.core.v3.QueryExecutorImpl.execute(QueryExecutorImpl.java:257)\ org.postgresql.jdbc2.AbstractJdbc2Statement.execute(AbstractJdbc2Statement.java:500)\ org.postgresql.jdbc2.AbstractJdbc2Statement.executeWithFlags(AbstractJdbc2Statement.java:388)\ org.postgresql.jdbc2.AbstractJdbc2Statement.execute(AbstractJdbc2Statement.java:381)\ scala.slick.jdbc.StatementInvoker.results(StatementInvoker.scala:38)\ scala.slick.jdbc.StatementInvoker.iteratorTo(StatementInvoker.scala:22)\ scala.slick.jdbc.Invoker$class.iterator(Invoker.scala:15)\ scala.slick.jdbc.StatementInvoker.iterator(StatementInvoker.scala:16)\ scala.slick.jdbc.Invoker$class.execute(Invoker.scala:23)\ scala.slick.jdbc.StatementInvoker.execute(StatementInvoker.scala:16)\ com.mindcandy.waterfall.io.RedshiftIOSink$$anonfun$storeFrom$1$$anonfun$apply$mcV$sp$2.apply$mcV$sp(RedshiftIO.scala:64)\ com.mindcandy.waterfall.io.RedshiftIOSink$$anonfun$storeFrom$1$$anonfun$apply$mcV$sp$2.apply(RedshiftIO.scala:56)\ com.mindcandy.waterfall.io.RedshiftIOSink$$anonfun$storeFrom$1$$anonfun$apply$mcV$sp$2.apply(RedshiftIO.scala:56)\ scala.util.DynamicVariable.withValue(DynamicVariable.scala:57)\ scala.slick.backend.DatabaseComponent$class.withDynamicSession(DatabaseComponent.scala:67)\ scala.slick.jdbc.JdbcBackend$.withDynamicSession(JdbcBackend.scala:448)\ scala.slick.backend.DatabaseComponent$DatabaseDef$$anonfun$withDynSession$1.apply(DatabaseComponent.scala:51)\ scala.slick.backend.DatabaseComponent$DatabaseDef$$anonfun$withDynSession$1.apply(DatabaseComponent.scala:51)\ scala.slick.backend.DatabaseComponent$DatabaseDef$class.withSession(DatabaseComponent.scala:34)\ scala.slick.jdbc.JdbcBackend$DatabaseFactoryDef$$anon$4.withSession(JdbcBackend.scala:61)\ scala.slick.backend.DatabaseComponent$DatabaseDef$class.withDynSession(DatabaseComponent.scala:51)\ scala.slick.jdbc.JdbcBackend$DatabaseFactoryDef$$anon$4.withDynSession(JdbcBackend.scala:61)\ com.mindcandy.waterfall.io.RedshiftIOSink$$anonfun$storeFrom$1.apply$mcV$sp(RedshiftIO.scala:56)\ com.mindcandy.waterfall.io.RedshiftIOSink$$anonfun$storeFrom$1.apply(RedshiftIO.scala:56)\ com.mindcandy.waterfall.io.RedshiftIOSink$$anonfun$storeFrom$1.apply(RedshiftIO.scala:56)\ scala.util.Try$.apply(Try.scala:161)\ com.mindcandy.waterfall.io.RedshiftIOSink.storeFrom(RedshiftIO.scala:56)\ com.mindcandy.waterfall.WaterfallDrop$$anonfun$run$1$$anonfun$apply$1.apply(WaterfallDrop.scala:23)\ com.mindcandy.waterfall.WaterfallDrop$$anonfun$run$1$$anonfun$apply$1.apply(WaterfallDrop.scala:22)\ scala.util.Success.flatMap(Try.scala:200)\ com.mindcandy.waterfall.WaterfallDrop$$anonfun$run$1.apply(WaterfallDrop.scala:22)\ com.mindcandy.waterfall.WaterfallDrop$$anonfun$run$1.apply(WaterfallDrop.scala:21)\ scala.util.Success.flatMap(Try.scala:200)\ com.mindcandy.waterfall.WaterfallDrop$class.run(WaterfallDrop.scala:21)\ com.mindcandy.waterfall.drop.GooglePlayToRedshiftDrop.run(GooglePlayToRedshiftDrop.scala:180)\ com.mindcandy.waterfall.actor.DropWorker$$anonfun$receive$1.applyOrElse(DropWorker.scala:19)\ akka.actor.Actor$class.aroundReceive(Actor.scala:465)\ com.mindcandy.waterfall.actor.DropWorker.aroundReceive(DropWorker.scala:14)\ akka.actor.ActorCell.receiveMessage(ActorCell.scala:516)\ akka.actor.ActorCell.invoke(ActorCell.scala:487)\ akka.dispatch.Mailbox.processMailbox(Mailbox.scala:238)\ akka.dispatch.Mailbox.run(Mailbox.scala:220)\ akka.dispatch.ForkJoinExecutorConfigurator$AkkaForkJoinTask.exec(AbstractDispatcher.scala:393)\ scala.concurrent.forkjoin.ForkJoinTask.doExec(ForkJoinTask.java:260)\ scala.concurrent.forkjoin.ForkJoinPool$WorkQueue.runTask(ForkJoinPool.java:1339)\ scala.concurrent.forkjoin.ForkJoinPool.runWorker(ForkJoinPool.java:1979)\ scala.concurrent.forkjoin.ForkJoinWorkerThread.run(ForkJoinWorkerThread.java:107)\ ",
"logOutput": null,
"endTime": "2014-11-20T05:56:39.475Z",
"runID": "6ab91b0b-2304-4c0e-a842-6dea1d665ed6",
"jobID": 23
},
{
"startTime": "2014-11-19T05:55:00.028Z",
"exception": null,
"logOutput": null,
"endTime": "2014-11-19T05:55:16.858Z",
"runID": "fa2f4c14-bd90-4622-987f-98495ac78e51",
"jobID": 23
},
{
"startTime": "2014-11-18T05:55:00.017Z",
"exception": null,
"logOutput": null,
"endTime": "2014-11-18T05:55:58.030Z",
"runID": "0336c2a7-4d73-4258-b06c-4006b722e553",
"jobID": 23
},
{
"startTime": "2014-11-17T05:55:00.018Z",
"exception": null,
"logOutput": null,
"endTime": "2014-11-17T05:56:06.427Z",
"runID": "6b524e1b-2d1e-41a1-b7b4-7af8e247f66e",
"jobID": 23
},
{
"startTime": "2014-11-16T05:55:00.079Z",
"exception": null,
"logOutput": null,
"endTime": "2014-11-16T05:56:19.877Z",
"runID": "2d7c12be-caff-42a5-9afc-f20d4cdef960",
"jobID": 23
},
{
"startTime": "2014-12-23T06:00:00.022Z",
"exception": null,
"logOutput": null,
"endTime": "2014-12-23T06:00:58.489Z",
"runID": "9b83be07-939a-4526-9ab7-c8ba91007d32",
"jobID": 3
},
{
"startTime": "2014-12-22T06:00:00.047Z",
"exception": null,
"logOutput": null,
"endTime": "2014-12-22T06:00:32.437Z",
"runID": "c39a73cd-ec4b-4b8c-97e3-b8f71b9ee0e1",
"jobID": 3
},
{
"startTime": "2014-12-21T06:00:00.070Z",
"exception": null,
"logOutput": null,
"endTime": "2014-12-21T06:00:23.216Z",
"runID": "6654a650-38da-43eb-a597-53988efda14b",
"jobID": 3
},
{
"startTime": "2014-12-20T06:00:00.012Z",
"exception": null,
"logOutput": null,
"endTime": "2014-12-20T06:00:24.368Z",
"runID": "e5fd9e0e-bca9-44f8-96ae-ee8b1d2418fd",
"jobID": 3
},
{
"startTime": "2014-12-19T06:00:00.022Z",
"exception": null,
"logOutput": null,
"endTime": "2014-12-19T06:00:19.462Z",
"runID": "033ec018-0b6b-40f0-ab44-41daa1fc395a",
"jobID": 3
},
{
"startTime": "2014-12-18T06:00:00.026Z",
"exception": null,
"logOutput": null,
"endTime": "2014-12-18T06:00:25.723Z",
"runID": "e8e990fa-493a-44f5-a8e9-52a7df194d30",
"jobID": 3
},
{
"startTime": "2014-12-17T06:00:00.025Z",
"exception": null,
"logOutput": null,
"endTime": "2014-12-17T06:02:23.398Z",
"runID": "50ad9b13-14a2-43ea-8afa-09588e65ec33",
"jobID": 3
}
            ]
        }


        /** fetch all jobs and their logs */
        $scope.fetchJobs = function() {
            $scope.lastFetch = new Date();
            for (i = 0; i < testJobs.jobs.length; i++) {
                var jsonJob = testJobs.jobs[i];
                testJobs.jobs[i] = fetchLogs(jsonJob)
            }
            $scope.jobs = testJobs.jobs;
            $scope.jobCount = testJobs.count;
            /*$http.get('http://sta-waterfall-app01.aws.mindcandy.com/jobs')
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
            // perform refresh after interval*/
            $timeout(function() { $scope.fetchJobs(); }, $scope.refreshInterval);
        };

        /* fetch the logs for the given job */
        function fetchLogs(jsonJob) {
            jsonJob['logData'] = {}
            jsonJob['logData']['logs'] = $.grep(testLogs.logs, function(e) { return e.jobID == jsonJob.jobID});
            jsonJob['status'] = jobStatus(jsonJob);
            /*$http.get('http://sta-waterfall-app01.aws.mindcandy.com/logs?jobid=' + jsonJob.jobID + "&period=168&limit=5")
                .success(function (data) {
                    jsonJob['logData'] = data;
                    jsonJob['status'] = jobStatus(jsonJob);
                })
                .error(function(data, status) {
                    console.error("failed to get logs for job " + jsonJob.jobID + "!");
                });*/
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
            var dat = new Date(this.valueOf())
            dat.setDate(dat.getDate() + days);
            return dat;
        }

        /* Get all dates between a start and end date */
        function getDateRange(startDate, stopDate) {
            var dateArray = new Array();
            var currentDate = startDate;
            while (currentDate <= stopDate) {
                dateArray.push(currentDate.getTime())
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
            };

            // Get range of categories to be shown in graph
            var dateRange = getDateRange(minDate, new Date());

            // Assign each series point to a category ID
            for (i = 0; i < seriesData.length; i++) {
                for (j = 0; j < seriesData[i]['data'].length; j++) {
                    var index = -1
                    for (k = 0; k < dateRange.length; k++) {
                        if (dateRange[k] == new Date(seriesData[i]['data'][j][0]).getTime()) {
                            index = k
                        }
                    }
                    seriesData[i]['data'][j][0] = index
                }
            };

            return {seriesData: seriesData, dateRange: dateRange}
        }

        /* Organise data into 1 value for each date */
        function getTotalRuntimeSeriesData(seriesData) {
            // seriesData is sorted, so minDate is first in array
            var minDate = seriesData[0]['date'];

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
            var outputArray = []
            for (i = 0; i < seriesData.length; i++) {
                outputArray.push(seriesData[i]['runtime'])
            }

            return {seriesData: outputArray, dateRange: dateRange}
        }

        /* Get jobs data from API */
        function getDropRuntimesChart() {
            // Get all logs for all jobs, formatted to be accepted by Highcharts
            var dataArray = new Array();
            for (i = 0; i < $scope.jobCount; i++) {
                var job = {};
                job['name'] = $scope.jobs[i]['name'];
                job['data'] = new Array();
                for (j = 0; j < $scope.jobs[i]['logData']['logs'].length; j++) {
                    var logs = $scope.jobs[i]['logData']['logs'][j];
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
            var chartData = getDropRuntimeSeriesData(seriesData)

            // Create the chart object
            return columnChartFormatting(chartData['seriesData'], chartData['dateRange'], 1, true, 500, 1500);
        }

        /* Get jobs data from API */
        function getTotalRuntimesChart() {
            // Get all logs for all jobs, formatted to be accepted by Highcharts
            var dataArray = new Array();
            for (i = 0; i < $scope.jobCount; i++) {
                for (j = 0; j < $scope.jobs[i]['logData']['logs'].length; j++) {
                    var jobLogs = $scope.jobs[i]['logData']['logs'][j];
                    var jobDate = Highcharts.dateFormat('%Y-%m-%d', new Date(jobLogs['endTime']));
                    var jobRuntime = new Date(jobLogs['endTime']).getTime() - new Date(jobLogs['startTime']).getTime();
                    var newDate = { date : jobDate, runtime : jobRuntime };

                    if (dataArray.length > 0) { // Dates have been added to dataArray
                        var containsDate = false;
                        for (k = 0; k < dataArray.length; k++) {
                            if (dataArray[k]['date'] === jobDate) { // dataArray already contains an entry for this date
                                dataArray[k]['runtime'] += jobRuntime
                                containsDate = true;
                                break;
                            };
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
            var chartData = getTotalRuntimeSeriesData(seriesData)

            // Create the chart object
            return columnChartFormatting([{data: chartData['seriesData']}], chartData['dateRange'], 3, false, 500, 700);
        }

        /* Run initial lookup */
        $scope.fetchJobs();

        /* Create charts */
        $scope.drop_runtime_chart = getDropRuntimesChart();
        $scope.total_runtime_chart = getTotalRuntimesChart();
    }]);
});
