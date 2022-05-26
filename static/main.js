// this is only a test
(function () {
  'use strict';

  angular.module('WordcountApp', [])

  .controller('WordcountController', ['$scope', '$log',
    function($scope, $log) {
      $scope.getResults = function() {
        $log.log("test");
      };
    }
  ]);

}());