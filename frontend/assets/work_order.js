/* The future! */
var setupButtonASpace = function() {
  $workOrder = $(AS.renderTemplate("workOrderButtonTemplate"));
  $('#other-dropdown .dropdown-menu').append($('<li />').append($workOrder));
  $('#other-dropdown').show();
};


// setup the work order toolbar action
$(document).on("loadedrecordform.aspace", function(event, $container) {
  if (infiniteTree.rootMeta.type == 'resource') {
    setupButtonASpace();
  }
});
