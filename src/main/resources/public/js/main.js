require.config({
    baseUrl: '/assets/js',
    paths: {
        'angular': 'lib/angular.min',
        'angular-route': 'lib/angular-route.min',
        'angularAMD': 'lib/angularAMD.min',
        'jquery': 'lib/jquery-1.11.1'
    },
    'shim' : {
        'angularAMD': ['angular'],
        'angular-route' : ['angular'],
        'bootstrap' : [ 'jquery']
    },
    deps: ['app']
});