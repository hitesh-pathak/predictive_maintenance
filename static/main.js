(function () {
  'use strict';

  angular.module('RULpredictor', [])

  .controller('RULpredictorController', ['$scope', '$log', '$http', '$timeout',
    function($scope, $log, $http, $timeout) {

      $scope.submitButtonText = 'Submit';
      $scope.loading = false;

      $scope.getResults = function() {

        $log.log("test");

        // get filepath
        var userInput = $scope.filepath;

        // fire api request
        $http.post('/start', {"filepath": userInput}).
          success(function(results) {
            $log.log(results);
            getStatus(results);
          }).
          error(function(error) {
            $log.log(error);
          });

    };

    function getStatus(jobID) {
      var timeout = "";

      var poller = function() {
        // fire polling request to get status
        $http.get('/results/'+jobID).
          success(function(data, status, headers, config) {
            if (status === 202) {
              $log.log(data, status);
            } else if (status === 200) {
                $log.log(data);
                $scope.status = data;
                $timeout.cancel(timeout);
                return false;
            }
            // continue to call the poller() every 2 seconds
            // until timeout is cancelled
            timeout = $timeout(poller, 2000);
          });
      };
      poller ();

    }

}]);

}());